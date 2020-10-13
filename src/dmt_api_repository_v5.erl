-module(dmt_api_repository_v5).

-behaviour(dmt_api_repository).

-include_lib("damsel/include/dmsl_domain_config_thrift.hrl").
-include_lib("mg_proto/include/mg_proto_state_processing_thrift.hrl").

-define(NS, <<"domain-config">>).
-define(ID, <<"primary/v5">>).
-define(BASE, 10).

%% API

-export([checkout/2]).
-export([pull/2]).
-export([pull/3]).
-export([commit/3]).

%% State processor

-behaviour(dmt_api_automaton_handler).

-export([process_call/3]).
-export([process_signal/3]).

%%
-record(st, {
    snapshot = #'Snapshot'{version = 0, domain = dmt_domain:new()} :: snapshot(),
    history = #{} :: dmt_api_repository:history()
}).

-type st() :: #st{}.
-type context() :: woody_context:ctx().
-type history_range() :: mg_proto_state_processing_thrift:'HistoryRange'().
-type machine() :: mg_proto_state_processing_thrift:'Machine'().
-type history() :: mg_proto_state_processing_thrift:'History'().

-type ref() :: dmsl_domain_config_thrift:'Reference'().
-type snapshot() :: dmt_api_repository:snapshot().
-type commit() :: dmt_api_repository:commit().

-spec checkout(ref(), context()) ->
    {ok, snapshot()}
    | {error, version_not_found}.
checkout({head, #'Head'{}}, Context) ->
    HistoryRange = #mg_stateproc_HistoryRange{
        'after' = undefined,
        'limit' = ?BASE,
        'direction' = backward
    },
    case get_history_by_range(HistoryRange, Context) of
        #st{} = St ->
            squash_state(St);
        {error, version_not_found} ->
            {error, version_not_found}
    end;
checkout({version, V}, Context) ->
    BaseV = get_base_version(V),
    HistoryRange = #mg_stateproc_HistoryRange{
        'after' = get_event_id(BaseV),
        'limit' = V - BaseV,
        'direction' = forward
    },
    case get_history_by_range(HistoryRange, Context) of
        #st{} = St ->
            case squash_state(St) of
                {ok, #'Snapshot'{version = V}} = Result ->
                    Result;
                {ok, _} ->
                    {error, version_not_found}
            end;
        {error, version_not_found} ->
            {error, version_not_found}
    end.

-spec pull(dmt_api_repository:version(), context()) ->
    {ok, dmt_api_repository:history()}
    | {error, version_not_found}.
pull(Version, Context) ->
    pull(Version, undefined, Context).

-spec pull(dmt_api_repository:version(), dmt_api_repository:limit(), context()) ->
    {ok, dmt_api_repository:history()}
    | {error, version_not_found}.
pull(Version, Limit, Context) ->
    After = get_event_id(Version),
    case get_history_by_range(#mg_stateproc_HistoryRange{'after' = After, 'limit' = Limit}, Context) of
        #st{history = History} ->
            {ok, History};
        {error, version_not_found} ->
            {error, version_not_found}
    end.

-spec commit(dmt_api_repository:version(), commit(), context()) ->
    {ok, snapshot()}
    | {error, version_not_found | {operation_error, dmt_domain:operation_error()}}.
commit(Version, Commit, Context) ->
    BaseID = get_event_id(get_base_version(Version)),
    decode_call_result(
        dmt_api_automaton_client:call(
            ?NS,
            ?ID,
            %% TODO in theory, it's enought ?BASE + 1 events here,
            %% but it's complicated and needs to be covered by tests
            #mg_stateproc_HistoryRange{'after' = BaseID},
            encode_call({commit, Version, Commit}),
            Context
        )
    ).

%%

-spec get_history_by_range(history_range(), context()) -> st() | {error, version_not_found}.
get_history_by_range(HistoryRange, Context) ->
    case dmt_api_automaton_client:get_history(?NS, ?ID, HistoryRange, Context) of
        {ok, History} ->
            read_history(History);
        {error, #mg_stateproc_MachineNotFound{}} ->
            ok = dmt_api_automaton_client:start(?NS, ?ID, Context),
            get_history_by_range(HistoryRange, Context);
        {error, #mg_stateproc_EventNotFound{}} ->
            {error, version_not_found}
    end.

%%

-spec process_call(dmt_api_automaton_handler:call(), machine(), context()) ->
    {dmt_api_automaton_handler:response(), dmt_api_automaton_handler:events()} | no_return().
process_call(Call, #mg_stateproc_Machine{ns = ?NS, id = ?ID} = Machine, Context) ->
    Args = decode_call(Call),
    {Result, Events} = handle_call(Args, read_history(Machine), Context),
    {encode_call_result(Result), encode_events(Events)};
process_call(_Call, #mg_stateproc_Machine{ns = NS, id = ID}, _Context) ->
    Message = <<"Unknown machine '", NS/binary, "' '", ID/binary, "'">>,
    woody_error:raise(system, {internal, resource_unavailable, Message}).

-spec process_signal(dmt_api_automaton_handler:signal(), machine(), context()) ->
    {dmt_api_automaton_handler:action(), dmt_api_automaton_handler:events()} | no_return().
process_signal({init, #mg_stateproc_InitSignal{}}, #mg_stateproc_Machine{ns = ?NS, id = ?ID}, _Context) ->
    {#mg_stateproc_ComplexAction{}, []};
process_signal({timeout, #mg_stateproc_TimeoutSignal{}}, #mg_stateproc_Machine{ns = ?NS, id = ?ID}, _Context) ->
    {#mg_stateproc_ComplexAction{}, []};
process_signal(_Signal, #mg_stateproc_Machine{ns = NS, id = ID}, _Context) ->
    Message = <<"Unknown machine '", NS/binary, "' '", ID/binary, "'">>,
    woody_error:raise(system, {internal, resource_unavailable, Message}).

%%

handle_call({commit, Version, Commit}, St, _Context) ->
    case squash_state(St) of
        {ok, #'Snapshot'{version = Version} = Snapshot} ->
            apply_commit(Snapshot, Commit);
        {ok, #'Snapshot'{version = V}} when V > Version ->
            % Is this retry? Maybe we already applied this commit.
            check_commit(Version, Commit, St);
        {ok, _} ->
            {{error, head_mismatch}, []}
    end.

apply_commit(#'Snapshot'{version = VersionWas, domain = DomainWas}, #'Commit'{ops = Ops} = Commit) ->
    case dmt_domain:apply_operations(Ops, DomainWas) of
        {ok, Domain} ->
            Snapshot = #'Snapshot'{version = VersionWas + 1, domain = Domain},
            {{ok, Snapshot}, [make_event(Snapshot, Commit)]};
        {error, Reason} ->
            {{error, {operation_error, Reason}}, []}
    end.

check_commit(Version, Commit, #st{snapshot = BaseSnapshot, history = History}) ->
    case maps:get(Version + 1, History) of
        Commit ->
            % it's ok, commit alredy applied, lets return this snapshot
            {dmt_history:travel(Version + 1, History, BaseSnapshot), []};
        _ ->
            {{error, head_mismatch}, []}
    end.

-spec read_history(machine() | history()) -> st().
read_history(#mg_stateproc_Machine{history = Events}) ->
    read_history(Events);
read_history(Events) ->
    read_history(Events, #st{}).

-spec read_history([mg_proto_state_processing_thrift:'Event'()], st()) -> st().
read_history([], St) ->
    St;
read_history(
    [#mg_stateproc_Event{id = Id, data = EventData, format_version = FmtVsn} | Rest],
    #st{history = History} = St
) ->
    {commit, Commit, Meta} = decode_event(FmtVsn, EventData),
    case Meta of
        #{snapshot := Snapshot} ->
            read_history(
                Rest,
                St#st{snapshot = Snapshot, history = History#{Id => Commit}}
            );
        #{} ->
            read_history(
                Rest,
                St#st{history = History#{Id => Commit}}
            )
    end.

squash_state(#st{snapshot = BaseSnapshot, history = History}) ->
    case dmt_history:head(History, BaseSnapshot) of
        {ok, Snapshot} ->
            {ok, Snapshot};
        {error, Error} ->
            error(Error)
    end.

%%

make_event(Snapshot, Commit) ->
    Meta =
        case (Snapshot#'Snapshot'.version) rem ?BASE of
            0 ->
                #{snapshot => Snapshot};
            _ ->
                #{}
        end,
    {commit, Commit, Meta}.

encode_events(Events) ->
    FmtVsn = 1,
    encode_events(FmtVsn, Events).

encode_events(FmtVsn, Events) ->
    [encode_event(FmtVsn, E) || E <- Events].

encode_event(FmtVsn, Data) ->
    #mg_stateproc_Content{format_version = FmtVsn, data = encode_event_data(FmtVsn, Data)}.

encode_event_data(1 = FmtVsn, {commit, Commit, Meta}) ->
    {arr, [{str, <<"commit">>}, encode(commit, Commit), encode_commit_meta(FmtVsn, Meta)]}.

encode_commit_meta(1, #{snapshot := Snapshot}) ->
    {obj, #{{str, <<"snapshot">>} => encode(snapshot, Snapshot)}};
encode_commit_meta(1, #{}) ->
    {obj, #{}}.

decode_event(1 = FmtVsn, {arr, [{str, <<"commit">>}, Commit, Meta]}) ->
    {commit, decode(commit, Commit), decode_commit_meta(FmtVsn, Meta)}.

decode_commit_meta(1, {obj, #{{str, <<"snapshot">>} := Snapshot}}) ->
    #{snapshot => decode(snapshot, Snapshot)};
decode_commit_meta(1, {obj, #{}}) ->
    #{}.

%%

encode_call({commit, Version, Commit}) ->
    {arr, [{str, <<"commit">>}, {i, Version}, encode(commit, Commit)]}.

decode_call({arr, [{str, <<"commit">>}, {i, Version}, Commit]}) ->
    {commit, Version, decode(commit, Commit)}.

encode_call_result({ok, Snapshot}) ->
    {arr, [{str, <<"ok">>}, encode(snapshot, Snapshot)]};
encode_call_result({error, Reason}) ->
    {arr, [{str, <<"err">>}, {bin, term_to_binary(Reason)}]}.

decode_call_result({arr, [{str, <<"ok">>}, Snapshot]}) ->
    {ok, decode(snapshot, Snapshot)};
decode_call_result({arr, [{str, <<"err">>}, {bin, Reason}]}) ->
    {error, binary_to_term(Reason)}.

%%

encode(T, V) ->
    {bin, dmt_api_thrift_utils:encode(binary, get_type_info(T), V)}.

decode(T, {bin, V}) ->
    dmt_api_thrift_utils:decode(binary, get_type_info(T), V).

get_type_info(commit) ->
    {struct, struct, {dmsl_domain_config_thrift, 'Commit'}};
get_type_info(snapshot) ->
    {struct, struct, {dmsl_domain_config_thrift, 'Snapshot'}}.

get_base_version(V) when is_integer(V) andalso V >= ?BASE ->
    (V div ?BASE) * ?BASE - 1;
get_base_version(V) when is_integer(V) ->
    0.

get_event_id(ID) when is_integer(ID) andalso ID > 0 ->
    ID;
get_event_id(0) ->
    undefined.
