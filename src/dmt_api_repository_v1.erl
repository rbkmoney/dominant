-module(dmt_api_repository_v1).
-behaviour(dmt_api_repository).

-include_lib("dmsl/include/dmsl_state_processing_thrift.hrl").
-include_lib("dmsl/include/dmsl_domain_config_thrift.hrl").

-define(NS  , <<"domain-config">>).
-define(ID  , <<"primary">>).

%% API

-export([get_history/2]).
-export([get_history/3]).
-export([commit/4]).

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
            #{};
        {error, #'EventNotFound'{}} ->
            {error, version_not_found}
    end.

%%

-spec commit(dmt_api_repository:version(), dmt_api_repository:commit(), dmt_api_repository:snapshot(), context()) ->
    {ok, dmt_api_repository:snapshot()} | {error, version_not_found | operation_conflict}.
commit(Version, Commit, _, Context) ->
    call({commit, Version, Commit}, Context).

%%

-define(NIL, {nl, #msgpack_Nil{}}).

-type commit_call()   :: {commit, dmt_api_repository:version(), dmt_api_repository:commit()}.
-type commit_result() :: {ok, dmt_api_repository:snapshot()} | {error, version_not_found | operation_conflict}.

-spec call(commit_call(), context()) ->
    commit_result() | no_return().
call(Call, Context) ->
    decode_call_result(dmt_api_automaton_client:call(?NS, ?ID, encode_call(Call), Context)).

%%

-spec handle_function(woody:func(), woody:args(), context(), woody:options()) ->
    {ok, woody:result()} | no_return().
handle_function('ProcessCall', [#'CallArgs'{arg = Payload, machine = Machine}], _Context, _Opts) ->
    Call = decode_call(Payload),
    {Result, Events} = handle_call(Call, read_history(Machine)),
    Response = encode_call_result(Result),
    {ok, construct_call_result(Response, [encode_event(E) || E <- Events])};
handle_function('ProcessSignal', [#'SignalArgs'{signal = {init, #'InitSignal'{}}}], _Context, _Opts) ->
    {ok, construct_signal_result([])}.

construct_call_result(Response, Events) ->
    #'CallResult'{
        response = Response,
        change = #'MachineStateChange'{aux_state = ?NIL, events = Events},
        action = #'ComplexAction'{}
    }.

construct_signal_result(Events) ->
    #'SignalResult'{
        change = #'MachineStateChange'{aux_state = ?NIL, events = Events},
        action = #'ComplexAction'{}
    }.

%%

handle_call({commit, Version, Commit}, History) ->
    case dmt_api:apply_commit(Version, Commit, #'Snapshot'{version = 0, domain = dmt_domain:new()}, History) of
        {ok, _} = Ok ->
            {Ok, [{commit, Commit}]};
        {error, version_not_found} ->
            {{error, version_not_found}, []};
        {error, Reason} ->
            _ = lager:info("commit failed: ~p", [Reason]),
            {{error, operation_conflict}, []}
    end.

%%

-spec read_history(history() | machine()) ->
    dmt_api_repository:history().
read_history(#'Machine'{history = Events}) ->
    read_history(Events);
read_history(Events) ->
    read_history(Events, #{}).

-spec read_history(dmsl_state_processing_thrift:'History'(), dmt_api_repository:history()) ->
    dmt_api_repository:history().
read_history([], History) ->
    History;
read_history([#'Event'{id = Id, event_payload = EventData} | Rest], History) ->
    {commit, Commit} = decode_event(EventData),
    read_history(Rest, History#{Id => Commit}).

%%

encode_event({commit, Commit}) ->
    {bin, term_to_binary(Commit)}.

decode_event({bin, CommitData}) ->
    {commit, binary_to_term(CommitData)}.

%%

encode_call(Call) ->
    {bin, term_to_binary(Call)}.

decode_call({bin, CallData}) ->
    binary_to_term(CallData).

encode_call_result(Result) ->
    {bin, term_to_binary(Result)}.

decode_call_result({bin, ResultData}) ->
    binary_to_term(ResultData).
