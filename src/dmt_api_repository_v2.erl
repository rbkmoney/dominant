-module(dmt_api_repository_v2).

%%
%% This is old, depricated version of domain repository.
%%

-include_lib("dmsl/include/dmsl_domain_config_thrift.hrl").
-include_lib("mg_proto/include/mg_proto_state_processing_thrift.hrl").

-define(NS  , <<"domain-config">>).
-define(ID  , <<"primary/v2">>).

%% API
-behaviour(dmt_api_repository).

-export([checkout/2]).
-export([pull/2]).
-export([commit/3]).

-export([get_history/2]).
-export([get_history/3]).

%% State processor
-behaviour(woody_server_thrift_handler).

-export([handle_function/4]).

%%

-type context()         :: woody_context:ctx().
-type history_range()   :: mg_proto_state_processing_thrift:'HistoryRange'().
-type machine()         :: mg_proto_state_processing_thrift:'Machine'().
-type history()         :: mg_proto_state_processing_thrift:'History'().

-type ref()             :: dmsl_domain_config_thrift:'Reference'().
-type snapshot()        :: dmt_api_repository:snapshot().
-type commit()          :: dmt_api_repository:commit().

%%

-spec checkout(ref(), context()) ->
    {ok, snapshot()} |
    {error, version_not_found}.

checkout({head, #'Head'{}}, Context) ->
    Snapshot = ensure_snapshot(dmt_api_cache:get_latest()),
    case get_history(Snapshot#'Snapshot'.version, undefined, Context) of
        {ok, History} ->
            dmt_history:head(History, Snapshot);
        {error, version_not_found} ->
            {error, version_not_found}
    end;
checkout({version, Version}, Context) ->
    ClosestSnapshot = ensure_snapshot(dmt_api_cache:get_closest(Version)),
    From = min(Version, ClosestSnapshot#'Snapshot'.version),
    Limit = abs(Version - ClosestSnapshot#'Snapshot'.version),
    case get_history(From, Limit, Context) of
        {ok, History} when map_size(History) =:= Limit ->
            %% TO DO: Need to fix dmt_history:travel. It can return {error, ...}
            {ok, Snapshot} = dmt_history:travel(Version, History, ClosestSnapshot),
            {ok, Snapshot};
        {ok, #{}} ->
            {error, version_not_found};
        {error, version_not_found} ->
            {error, version_not_found}
    end.

-spec pull(dmt_api_repository:version(), context()) ->
    {ok, dmt_api_repository:history()} |
    {error, version_not_found}.

pull(Version, Context) ->
    After = get_event_id(Version),
    case get_history_by_range(#mg_stateproc_HistoryRange{'after' = After}, Context) of
        History when is_map(History) ->
            {ok, History};
        Error ->
            Error
    end.

-spec commit(dmt_api_repository:version(), commit(), context()) ->
    {ok, snapshot()} |
    {error, version_not_found | {operation_conflict, dmt_api_repository:operation_conflict()}}.

commit(Version, Commit, Context) ->
    decode_call_result(dmt_api_automaton_client:call(
        ?NS,
        ?ID,
        #mg_stateproc_HistoryRange{'after' = undefined},
        encode_call({commit, Version, Commit}),
        Context
    )).
%%

-define(NIL, {nl, #mg_msgpack_Nil{}}).

-spec handle_function(woody:func(), woody:args(), context(), woody:options()) ->
    {ok, woody:result()} | no_return().

handle_function('ProcessCall', [#mg_stateproc_CallArgs{arg = Payload, machine = Machine}], Context, _Opts) ->
    Call = decode_call(Payload),
    {Result, Events} = handle_call(Call, read_history(Machine), Context),
    {ok, construct_call_result(Result, Events)};
handle_function(
    'ProcessSignal',
    [#mg_stateproc_SignalArgs{
        signal = {init, #mg_stateproc_InitSignal{}}
    }],
    _Context,
    _Opts
) ->
    % No migration here, just start empty machine
    {ok, #mg_stateproc_SignalResult{
        change = #mg_stateproc_MachineStateChange{aux_state = ?NIL, events = []},
        action = #mg_stateproc_ComplexAction{}
    }}.

construct_call_result(Response, Events) ->
    #mg_stateproc_CallResult{
        response = encode_call_result(Response),
        change = #mg_stateproc_MachineStateChange{aux_state = ?NIL, events = encode_events(Events)},
        action = #mg_stateproc_ComplexAction{}
    }.

encode_events(Events) ->
    [encode_event(E) || E <- Events].

%%

handle_call({commit, Version, Commit}, History, _Context) ->
    Snapshot0 = ensure_snapshot(dmt_api_cache:get_latest()),
    case dmt_history:head(History, Snapshot0) of
        {ok, #'Snapshot'{version = Version} = Snapshot} ->
            apply_commit(Snapshot, Commit);
        {ok, _} ->
            {{error, head_mismatch}, []};
        {error, _} = Error ->
            {Error, []}
    end.

apply_commit(#'Snapshot'{version = VersionWas, domain = DomainWas}, #'Commit'{ops = Ops} = Commit) ->
    case dmt_domain:apply_operations(Ops, DomainWas) of
        {ok, Domain} ->
            Snapshot = #'Snapshot'{version = VersionWas + 1, domain = Domain},
            {{ok, Snapshot}, [{commit, Commit}]};
        {error, Reason} ->
            {{error, {operation_conflict, Reason}}, []}
    end.

%%
-spec get_history(pos_integer() | undefined, context()) ->
    dmt_api_repository:history().
get_history(Limit, Context) ->
    get_history_by_range(#'mg_stateproc_HistoryRange'{'after' = undefined, 'limit' = Limit}, Context).

-spec get_history(dmt_api_repository:version(), pos_integer() | undefined, context()) ->
    {ok, dmt_api_repository:history()} | {error, version_not_found}.
get_history(Version, Limit, Context) ->
    After = get_event_id(Version),
    case get_history_by_range(#'mg_stateproc_HistoryRange'{'after' = After, 'limit' = Limit}, Context) of
        History when is_map(History) ->
            {ok, History};
        Error ->
            Error
    end.

get_event_id(ID) when is_integer(ID) andalso ID > 0 ->
    ID;
get_event_id(0) ->
    undefined.

-spec get_history_by_range(history_range(), context()) ->
    dmt_api_repository:history() | {error, version_not_found}.
get_history_by_range(HistoryRange, Context) ->
    case dmt_api_automaton_client:get_history(?NS, ?ID, HistoryRange, Context) of
        {ok, History} ->
            read_history(History);
        {error, #'mg_stateproc_MachineNotFound'{}} ->
            #{};
        {error, #'mg_stateproc_EventNotFound'{}} ->
            {error, version_not_found}
    end.

ensure_snapshot({ok, Snapshot}) ->
    Snapshot;
ensure_snapshot({error, version_not_found}) ->
    #'Snapshot'{version = 0, domain = dmt_domain:new()}.

-spec read_history(machine() | history()) ->
    dmt_api_repository:history().
read_history(#'mg_stateproc_Machine'{history = Events}) ->
    read_history(Events);
read_history(Events) ->
    read_history(Events, #{}).

-spec read_history([mg_proto_state_processing_thrift:'Event'()], dmt_api_repository:history()) ->
    dmt_api_repository:history().
read_history([], History) ->
    History;
read_history([#'mg_stateproc_Event'{id = Id, event_payload = EventData} | Rest], History) ->
    {commit, Commit} = decode_event(EventData),
    read_history(Rest, History#{Id => Commit}).

%%

encode_event({commit, Commit}) ->
    {arr, [{str, <<"commit">>}, encode(commit, Commit)]}.

decode_event({arr, [{str, <<"commit">>}, Commit]}) ->
    {commit, decode(commit, Commit)}.

%%

encode_call({commit, Version, Commit}) ->
    {arr, [{str, <<"commit">>}, {i, Version}, encode(commit, Commit)]}.

decode_call({arr, [{str, <<"commit">>}, {i, Version}, Commit]}) ->
    {commit, Version, decode(commit, Commit)}.

encode_call_result({ok, Snapshot}) ->
    {arr, [{str, <<"ok">> }, encode(snapshot, Snapshot)]};
encode_call_result({error, Reason}) ->
    {arr, [{str, <<"err">>}, {bin, term_to_binary(Reason)}]}.

decode_call_result({arr, [{str, <<"ok">> }, Snapshot]}) ->
    {ok, decode(snapshot, Snapshot)};
decode_call_result({arr, [{str, <<"err">>}, {bin, Reason}]}) ->
    {error, binary_to_term(Reason)}.

%%

decode(T, V) ->
    dmt_api_thrift_utils:decode(msgpack, get_type_info(T), V).

encode(T, V) ->
    dmt_api_thrift_utils:encode(msgpack, get_type_info(T), V).

get_type_info(commit) ->
    {struct, struct, {dmsl_domain_config_thrift, 'Commit'}};
get_type_info(snapshot) ->
    {struct, struct, {dmsl_domain_config_thrift, 'Snapshot'}}.
