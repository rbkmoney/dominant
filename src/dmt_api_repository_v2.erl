-module(dmt_api_repository_v2).
-behaviour(dmt_api_repository).

-include_lib("dmsl/include/dmsl_state_processing_thrift.hrl").

-define(NS  , <<"domain-config">>).
-define(ID  , <<"primary/v2">>).

%% API

-export([get_history/2]).
-export([get_history_since/2]).
-export([commit/3]).

%% State processor

-behaviour(woody_server_thrift_handler).

-export([handle_function/4]).

%%

-type context() :: woody_context:ctx().

-spec get_history(pos_integer() | undefined, context()) ->
    dmt:history().
get_history(Limit, Context) ->
    get_history_by_range(#'HistoryRange'{'after' = undefined, 'limit' = Limit}, Context).

-spec get_history_since(dmt:version(), context()) ->
    {ok, dmt:history()} | {error, version_not_found}.
get_history_since(Version, Context) ->
    case get_history_by_range(#'HistoryRange'{'after' = Version, 'limit' = undefined}, Context) of
        History when is_map(History) ->
            {ok, History};
        Error ->
            Error
    end.

%%

-type history_range() :: dmsl_state_processing_thrift:'HistoryRange'().
-type machine()       :: dmsl_state_processing_thrift:'Machine'().
-type history()       :: dmsl_state_processing_thrift:'History'().

-spec get_history_by_range(history_range(), context()) ->
    dmt:history() | {error, version_not_found}.
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

-spec commit(dmt:version(), dmt:commit(), context()) ->
    {ok, dmt:snapshot()} | {error, version_not_found | operation_conflict}.
commit(Version, Commit, Context) ->
    call({commit, Version, Commit}, Context).

%%

-define(NIL, {nl, #msgpack_Nil{}}).

-type commit_call()   :: {commit, dmt:version(), dmt:commit()}.
-type commit_result() :: {ok, dmt:snapshot()} | {error, version_not_found | operation_conflict}.

-spec call(commit_call(), context()) ->
    commit_result() | no_return().
call(Call, Context) ->
    decode_call_result(Call, dmt_api_automaton_client:call(?NS, ?ID, encode_call(Call), Context)).

%%

-spec handle_function(woody:func(), woody:args(), context(), woody:options()) ->
    {ok, woody:result()} | no_return().
handle_function('ProcessCall', [#'CallArgs'{arg = Payload, machine = Machine}], _Context, _Opts) ->
    Call = decode_call(Payload),
    {Result, Events} = handle_call(Call, read_history(Machine)),
    {ok, construct_call_result(Call, Result, Events)};
handle_function('ProcessSignal', [#'SignalArgs'{signal = {init, #'InitSignal'{}}}], Context, _Opts) ->
    LegacyHistory = dmt_api_repository_v1:get_history(undefined, Context),
    {ok, construct_signal_result(get_events_from_history(LegacyHistory))}.

get_events_from_history(History) ->
    [{commit, Commit} || {_Version, Commit} <- lists:keysort(1, maps:to_list(History))].

construct_call_result(Call, Response, Events) ->
    #'CallResult'{
        response = encode_call_result(Call, Response),
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

handle_call({commit, Version, Commit}, History) ->
    case dmt_api:apply_commit(Version, Commit, History) of
        {ok, _} = Ok ->
            {Ok, [{commit, Commit}]};
        {error, version_not_found} ->
            {{error, version_not_found}, []};
        {error, Reason} ->
            _ = lager:info("commit failed: ~p", [Reason]),
            {{error, operation_conflict}, []}
    end.

%%

-spec read_history(machine() | history()) ->
    dmt:history().
read_history(#'Machine'{history = Events}) ->
    read_history(Events);
read_history(Events) ->
    read_history(Events, #{}).

-spec read_history([dmsl_state_processing_thrift:'Event'()], dmt:history()) ->
    dmt:history().
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

encode_call_result({commit, _, _}, {ok, Snapshot}) ->
    {arr, [{str, <<"ok">> }, encode(snapshot, Snapshot)]};
encode_call_result({commit, _, _}, {error, Reason}) ->
    {arr, [{str, <<"err">>}, {str, atom_to_binary(Reason, utf8)}]}.

decode_call_result({commit, _, _}, {arr, [{str, <<"ok">> }, Snapshot]}) ->
    {ok, decode(snapshot, Snapshot)};
decode_call_result({commit, _, _}, {arr, [{str, <<"err">>}, {str, Reason}]}) ->
    {error, binary_to_existing_atom(Reason, utf8)}.

%%

decode(T, V) ->
    dmt_api_thrift_utils:decode(msgpack, get_type_info(T), V).

encode(T, V) ->
    dmt_api_thrift_utils:encode(msgpack, get_type_info(T), V).

get_type_info(commit) ->
    {struct, struct, {dmsl_domain_config_thrift, 'Commit'}};
get_type_info(snapshot) ->
    {struct, struct, {dmsl_domain_config_thrift, 'Snapshot'}}.
