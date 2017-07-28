-module(dmt_api_repository_v2).
-behaviour(dmt_api_repository).

-include_lib("dmsl/include/dmsl_state_processing_thrift.hrl").

-define(NS  , <<"domain-config">>).
-define(ID  , <<"primary/v2">>).

%% API

-export([get_history/2]).
-export([get_history/3]).
-export([commit/3]).

%% State processor

-behaviour(woody_server_thrift_handler).

-export([handle_function/4]).

%%

-type context() :: woody_context:ctx().

-spec get_history(pos_integer() | undefined, context()) ->
    dmt_api_repository:history().
get_history(Limit, Context) ->
    get_history_by_range(#'HistoryRange'{'after' = undefined, 'limit' = Limit}, Context).

-spec get_history(dmt_api_repository:version(), pos_integer() | undefined, context()) ->
    {ok, dmt_api_repository:history()} | {error, version_not_found}.
get_history(Version, Limit, Context) ->
    After = get_event_id(Version),
    case get_history_by_range(#'HistoryRange'{'after' = After, 'limit' = Limit}, Context) of
        History when is_map(History) ->
            {ok, History};
        Error ->
            Error
    end.

get_event_id(ID) when is_integer(ID) andalso ID > 0 ->
    ID;
get_event_id(0) ->
    undefined.

%%

-type history_range() :: dmsl_state_processing_thrift:'HistoryRange'().
-type machine()       :: dmsl_state_processing_thrift:'Machine'().
-type history()       :: dmsl_state_processing_thrift:'History'().

-spec get_history_by_range(history_range(), context()) ->
    dmt_api_repository:history() | {error, version_not_found}.
get_history_by_range(HistoryRange, Context) ->
    case dmt_api_automaton_client:get_history(?NS, ?ID, HistoryRange, Context) of
        {ok, History} ->
            read_history(History);
        {error, #'MachineNotFound'{}} ->
            ok = dmt_api_automaton_client:start(?NS, ?ID, Context),
            get_history_by_range(HistoryRange, Context);
        {error, #'EventNotFound'{}} ->
            {error, version_not_found}
    end.

%%

-spec commit(dmt_api_repository:version(), dmt_api_repository:commit(), context()) ->
    {ok, dmt_api_repository:snapshot()} |
    {error, version_not_found | {operation_conflict, dmt_api_repository:operation_conflict()}}.
commit(Version, Commit, Context) ->
    decode_call_result(dmt_api_automaton_client:call(
        ?NS,
        ?ID,
        #'HistoryRange'{'after' = get_event_id(Version)},
        encode_call({commit, Version, Commit}),
        Context
    )).

%%

-define(NIL, {nl, #msgpack_Nil{}}).

-spec handle_function(woody:func(), woody:args(), context(), woody:options()) ->
    {ok, woody:result()} | no_return().
handle_function('ProcessCall', [#'CallArgs'{arg = Payload, machine = Machine}], Context, _Opts) ->
    Call = decode_call(Payload),
    {Result, Events} = handle_call(Call, read_history(Machine), Context),
    {ok, construct_call_result(Result, Events)};
handle_function('ProcessSignal', [#'SignalArgs'{signal = {init, #'InitSignal'{}}}], Context, _Opts) ->
    %%% TODO It's generally prettier to make up a _migrating_ repository which is the special repository
    %%%      module designed to facilitate migrations between some preconfigured 'old' repository backend
    %%%      and some 'new' one. The migration process could be triggered by the very first mutating
    %%%      operation (e.g. commit) going into this backend for example.
    LegacyHistory = dmt_api_repository_v1:get_history(undefined, Context),
    {ok, construct_signal_result(get_events_from_history(LegacyHistory))}.

get_events_from_history(History) ->
    [{commit, Commit} || {_Version, Commit} <- lists:keysort(1, maps:to_list(History))].

construct_call_result(Response, Events) ->
    #'CallResult'{
        response = encode_call_result(Response),
        change = #'MachineStateChange'{aux_state = ?NIL, events = encode_events(Events)},
        action = #'ComplexAction'{}
    }.

construct_signal_result(Events) ->
    #'SignalResult'{
        change = #'MachineStateChange'{aux_state = ?NIL, events = encode_events(Events)},
        action = #'ComplexAction'{}
    }.

encode_events(Events) ->
    [encode_event(E) || E <- Events].

%%

handle_call({commit, Version, Commit}, History, Context) ->
    case dmt_api_repository:apply_commit(Version, Commit, History, ?MODULE, Context) of
        {ok, _} = Ok ->
            {Ok, [{commit, Commit}]};
        {error, version_not_found} ->
            {{error, version_not_found}, []};
        {error, Reason} ->
            _ = lager:info("commit failed: ~p", [Reason]),
            {{error, {operation_conflict, Reason}}, []}
    end.

%%

-spec read_history(machine() | history()) ->
    dmt_api_repository:history().
read_history(#'Machine'{history = Events}) ->
    read_history(Events);
read_history(Events) ->
    read_history(Events, #{}).

-spec read_history([dmsl_state_processing_thrift:'Event'()], dmt_api_repository:history()) ->
    dmt_api_repository:history().
read_history([], History) ->
    History;
read_history([#'Event'{id = Id, event_payload = EventData} | Rest], History) ->
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
