-module(dmt_api_automaton_client).
-include_lib("dmsl/include/dmsl_state_processing_thrift.hrl").

-export([call/4]).
-export([get_history/4]).

%%

-type ns()            :: dmsl_base_thrift:'Namespace'().
-type id()            :: dmsl_base_thrift:'ID'().
-type call_args()     :: dmsl_state_processing_thrift:'CallArgs'().
-type call_result()   :: dmsl_state_processing_thrift:'CallResult'().
-type descriptor()    :: dmsl_state_processing_thrift:'MachineDescriptor'().
-type history_range() :: dmsl_state_processing_thrift:'HistoryRange'().
-type history()       :: dmsl_state_processing_thrift:'History'().
-type context()       :: woody_context:ctx().

%%

-spec call(ns(), id(), call_args(), context()) ->
    call_result() | no_return().
call(NS, ID, Args, Context) ->
    Descriptor = construct_descriptor(NS, ID, #'HistoryRange'{}),
    case issue_rpc('Call', [Descriptor, Args], Context) of
        {ok, Result} ->
            Result;
        {error, #'MachineNotFound'{}} ->
            ok = start(NS, ID, Context),
            call(NS, ID, Args, Context)
    end.

-spec get_history(ns(), id(), history_range(), context()) ->
    {ok, history()} | {error, dmsl_state_processing_thrift:'EventNotFound'()} | no_return().
get_history(NS, ID, HistoryRange, Context) ->
    Descriptor = construct_descriptor(NS, ID, HistoryRange),
    case issue_rpc('GetMachine', [Descriptor], Context) of
        {ok, #'Machine'{history = History}} ->
            {ok, History};
        {error, _} = Error ->
            Error
    end.

%%

-spec start(ns(), id(), context()) ->
    ok | no_return().
start(NS, ID, Context) ->
    case issue_rpc('Start', [NS, ID, {nl, #msgpack_Nil{}}], Context) of
        {ok, _} ->
            ok;
        {error, #'MachineAlreadyExists'{}} ->
            ok
    end.

-spec construct_descriptor(ns(), id(), history_range()) ->
    descriptor().
construct_descriptor(NS, ID, HistoryRange) ->
    #'MachineDescriptor'{
        ns = NS,
        ref = {id, ID},
        range = HistoryRange
    }.

-spec issue_rpc(atom(), list(term()), context()) ->
    term() | no_return().
issue_rpc(Method, Args, Context) ->
    Request = {{dmsl_state_processing_thrift, 'Automaton'}, Method, Args},
    {ok, URL} = application:get_env(dmt_api, automaton_service_url),
    Opts = #{url => URL, event_handler => {woody_event_handler_default, undefined}},
    case woody_client:call(Request, Opts, Context) of
        {ok, _} = Ok ->
            Ok;
        {exception, #'NamespaceNotFound'{}} ->
            error(namespace_not_found);
        {exception, #'MachineFailed'{}} ->
            error(machine_failed);
        {exception, Exception} ->
            {error, Exception}
    end.
