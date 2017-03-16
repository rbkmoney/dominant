-module(dmt_api_repository).

%% API

-export([get_commit/2]).
-export([get_history/2]).
-export([get_history_since/2]).
-export([commit/3]).

%% State processor

-behaviour(woody_server_thrift_handler).

-export([handle_function/4]).

%%

-include_lib("dmsl/include/dmsl_state_processing_thrift.hrl").

-type context() :: woody_context:ctx().

-spec get_commit(dmt:version(), context()) ->
    {ok, dmt:commit()} | {error, version_not_found}.
get_commit(ID, Context) ->
    HistoryRange = #'HistoryRange'{'after' = get_prev_commit(ID), 'limit' = 1},
    case get_history_by_range(HistoryRange, Context) of
        #{ID := Commit} ->
            {ok, Commit};
        #{} ->
            {error, version_not_found};
        Error ->
            Error
    end.

get_prev_commit(1) ->
    undefined;
get_prev_commit(N) ->
    N - 1.

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

-spec get_history_by_range(history_range(), context()) ->
    dmt:history() | {error, version_not_found}.
get_history_by_range(HistoryRange, Context) ->
    Descriptor = construct_descriptor(HistoryRange),
    try read_history(issue_rpc('GetMachine', [Descriptor], Context)) catch
        #'EventNotFound'{} ->
            {error, version_not_found}
    end.

%%

-spec commit(dmt:version(), dmt:commit(), context()) ->
    {ok, dmt:snapshot()} | {error, version_not_found | operation_conflict}.
commit(Version, Commit, Context) ->
    call({commit, Version, Commit}, Context).

%%

-define(NS  , <<"domain-config">>).
-define(ID  , <<"primary">>).
-define(REF , {id, ?ID}).

-define(NIL, {nl, #msgpack_Nil{}}).

-type commit_call()   :: {commit, dmt:version(), dmt:commit()}.
-type commit_result() :: {ok, dmt:snapshot()} | {error, version_not_found | operation_conflict}.

-spec call(commit_call(), context()) ->
    commit_result() | no_return().
call(Call, Context) ->
    Descriptor = construct_descriptor(#'HistoryRange'{}),
    decode_call_result(Call, issue_rpc('Call', [Descriptor, encode_call(Call)], Context)).

-spec start(context()) ->
    ok | no_return().
start(Context) ->
    try issue_rpc('Start', [?NS, ?ID, ?NIL], Context) catch
        #'MachineAlreadyExists'{} ->
            ok
    end.

-spec issue_rpc(atom(), list(term()), context()) ->
    term() | no_return().
issue_rpc(Method, Args, Context) ->
    Request = {{dmsl_state_processing_thrift, 'Automaton'}, Method, Args},
    {ok, URL} = application:get_env(dmt_api, automaton_service_url),
    Opts = #{url => URL, event_handler => {woody_event_handler_default, undefined}},
    case woody_client:call(Request, Opts, Context) of
        {ok, Result} ->
            Result;
        {exception, #'MachineNotFound'{}} ->
            ok = start(Context),
            case woody_client:call(Request, Opts, Context) of
                {ok, Result} ->
                    Result;
                {exception, Exception} ->
                    throw(Exception)
            end;
        {exception, Exception} ->
            throw(Exception)
    end.

%%

-type ns()            :: dmsl_base_thrift:'Namespace'().
-type ref()           :: dmsl_state_processing_thrift:'Reference'().
-type descriptor()    :: dmsl_state_processing_thrift:'MachineDescriptor'().

-spec construct_descriptor(history_range()) ->
    descriptor().
construct_descriptor(HistoryRange) ->
    construct_descriptor(?NS, ?REF, HistoryRange).

-spec construct_descriptor(ns(), ref(), history_range()) ->
    descriptor().
construct_descriptor(NS, Ref, HistoryRange) ->
    #'MachineDescriptor'{
        ns = NS,
        ref = Ref,
        range = HistoryRange
    }.

%%

-spec handle_function(woody:func(), woody:args(), context(), woody:options()) ->
    {ok, woody:result()} | no_return().
handle_function('ProcessCall', [#'CallArgs'{arg = Payload, machine = Machine}], _Context, _Opts) ->
    Call = decode_call(Payload),
    {Result, Events} = handle_call(Call, read_history(Machine)),
    Response = encode_call_result(Call, Result),
    {ok, construct_call_result(Response, [encode_event(E) || E <- Events])};
handle_function('ProcessSignal', [#'SignalArgs'{signal = {init, #'InitSignal'{}}}], _Context, _Opts) ->
    {ok, construct_signal_result([])};
handle_function('ProcessSignal', [#'SignalArgs'{signal = {repair, #'RepairSignal'{}}}], _Context, _Opts) ->
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
    case dmt_api:apply_commit(Version, Commit, History) of
        {ok, _} = Ok ->
            {Ok, [Commit]};
        {error, version_not_found} ->
            {{error, version_not_found}, []};
        {error, Reason} ->
            _ = lager:info("commit failed: ~p", [Reason]),
            {{error, operation_conflict}, []}
    end.

%%

-spec read_history(dmsl_state_processing_thrift:'Machine'()) ->
    dmt:history().
read_history(#'Machine'{history = Events}) ->
    read_history(Events, #{}).

-spec read_history([dmsl_state_processing_thrift:'Event'()], dmt:history()) ->
    dmt:history().
read_history([], History) ->
    History;
read_history([#'Event'{id = Id, event_payload = EventData} | Rest], History) ->
    read_history(Rest, History#{Id => decode_event(EventData)}).

%%

encode_event(Event) ->
    {bin, term_to_binary(Event)}.

decode_event({bin, EventData}) ->
    binary_to_term(EventData).

encode_call({commit, Version, Commit}) ->
    {arr, [{str, <<"commit">>}, {i, Version}, {bin, term_to_binary(Commit)}]}.

decode_call({arr, [{str, <<"commit">>}, {i, Version}, {bin, CommitData}]}) ->
    {commit, Version, binary_to_term(CommitData)}.

encode_call_result({commit, _, _}, {ok, Snapshot}) ->
    {arr, [{str, <<"ok">> } , {bin, term_to_binary(Snapshot)}]};
encode_call_result({commit, _, _}, {error, Reason}) ->
    {arr, [{str, <<"err">>} , {str, atom_to_binary(Reason, utf8)}]}.

decode_call_result({commit, _, _}, {arr, [{str, <<"ok">> }, {bin, SnapshotData}]}) ->
    {ok, binary_to_term(SnapshotData)};
decode_call_result({commit, _, _}, {arr, [{str, <<"err">>}, {str, Reason}]}) ->
    {error, binary_to_existing_atom(Reason, utf8)}.
