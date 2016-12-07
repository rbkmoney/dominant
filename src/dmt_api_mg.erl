-module(dmt_api_mg).

-export([start/1]).
-export([get_commit/2]).
-export([get_history/1]).
-export([get_history/3]).
-export([commit/3]).
-export([read_history/1]).

-export([call/3]).

-include_lib("dmsl/include/dmsl_state_processing_thrift.hrl").
-include_lib("dmsl/include/dmsl_base_thrift.hrl").

-define(NS  , <<"domain-config">>).
-define(ID  , <<"primary">>).
-define(REF , {id, ?ID}).

-type context() :: woody_client:context().
-type ref()           :: dmsl_state_processing_thrift:'Reference'().
-type ns()            :: dmsl_base_thrift:'Namespace'().
-type history_range() :: dmsl_state_processing_thrift:'HistoryRange'().

-type descriptor()    :: dmsl_state_processing_thrift:'MachineDescriptor'().

%%

-spec start(context()) ->
    {ok, context()} | no_return().
start(Context) ->
    try call('Start', [?NS, ?ID, <<>>], Context) catch
        {{exception, #'MachineAlreadyExists'{}}, Context1} ->
            {ok, Context1}
    end.

-spec get_commit(dmt:version(), context()) ->
    {dmt:commit() | {error, version_not_found}, context()} | no_return().
get_commit(ID, Context) ->
    dmt_api_context:map(
        get_history(get_prev_commit(ID), 1, Context),
        fun
            (#{ID := Commit}) -> Commit;
            (#{})             -> {error, version_not_found};
            (Error)           -> Error
        end
    ).

get_prev_commit(1) ->
    undefined;
get_prev_commit(N) ->
    N - 1.

%% TODO: add range requests after they are fixed in mg
-spec get_history(context()) ->
    {dmt:history(), context()}.
get_history(Context) ->
    get_history(undefined, undefined, Context).

%% TODO: change this interface to accept dmt:version only
-spec get_history(dmt:version() | undefined, pos_integer() | undefined, context()) ->
    {dmt:history() | {error, version_not_found}, context()}.
get_history(After, Limit, Context) ->
    Range = #'HistoryRange'{'after' = prepare_event_id(After), 'limit' = Limit},
    Descriptor = prepare_descriptor(?NS, ?REF, Range),
    try dmt_api_context:map(call('GetMachine', [Descriptor], Context), fun read_history/1) catch
        {{exception, #'EventNotFound'{}}, Context1} ->
            {{error, version_not_found}, Context1}
    end.

-spec commit(dmt:version(), dmt:commit(), context()) ->
    {dmt:version() | {error, version_not_found | operation_conflict}, context()}.
commit(Version, Commit, Context) ->
    Descriptor = prepare_descriptor(?NS, ?REF, #'HistoryRange'{}),
    Call = term_to_binary({commit, Version, Commit}),
    dmt_api_context:map(call('Call', [Descriptor, Call], Context), fun binary_to_term/1).

%%

-spec call(atom(), list(term()), context()) ->
     {ok, context()} | {{ok, term()}, context()} | no_return().
call(Method, Args, Context) ->
    Request = {{dmsl_state_processing_thrift, 'Automaton'}, Method, Args},
    {ok, URL} = application:get_env(dmt_api, automaton_service_url),
    try
        woody_client:call(Context, Request, #{url => URL})
    catch
        throw:{{exception, #'MachineNotFound'{}}, Context1} ->
            {ok, Context2} = start(Context1),
            woody_client:call(Context2, Request, #{url => URL})
    end.

%% utils

-spec read_history(dmsl_state_processing_thrift:'Machine'()) -> dmt:history().
read_history(#'Machine'{history = Events}) ->
    read_history(Events, #{}).

-spec read_history([dmsl_state_processing_thrift:'Event'()], dmt:history()) ->
    dmt:history().
read_history([], History) ->
    History;
read_history([#'Event'{id = Id, event_payload = BinaryPayload} | Rest], History) ->
    read_history(Rest, History#{Id => binary_to_term(BinaryPayload)}).

prepare_event_id(ID) when is_integer(ID) andalso ID > 0 ->
    ID;
prepare_event_id(_) ->
    undefined.


-spec prepare_descriptor(ns(), ref(), history_range()) -> descriptor().
prepare_descriptor(NS, Ref, Range) ->
    #'MachineDescriptor'{
        ns = NS,
        ref = Ref,
        range = Range
    }.
