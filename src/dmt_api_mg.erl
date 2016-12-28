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
    ok | no_return().
start(Context) ->
    try call('Start', [?NS, ?ID, <<>>], Context) catch
        #'MachineAlreadyExists'{} ->
            ok
    end.

-spec get_commit(dmt:version(), context()) ->
    dmt:commit() | {error, version_not_found} | no_return().
get_commit(ID, Context) ->
    case get_history(get_prev_commit(ID), 1, Context) of
        #{ID := Commit} ->
            Commit;
        #{} ->
            {error, version_not_found};
        Error -> 
            Error
    end.

get_prev_commit(1) ->
    undefined;
get_prev_commit(N) ->
    N - 1.

%% TODO: add range requests after they are fixed in mg
-spec get_history(context()) ->
    dmt:history().
get_history(Context) ->
    get_history(undefined, undefined, Context).

%% TODO: change this interface to accept dmt:version only
-spec get_history(dmt:version() | undefined, pos_integer() | undefined, context()) ->
    dmt:history() | {error, version_not_found}.
get_history(After, Limit, Context) ->
    Range = #'HistoryRange'{'after' = prepare_event_id(After), 'limit' = Limit},
    Descriptor = prepare_descriptor(?NS, ?REF, Range),
    try read_history(call('GetMachine', [Descriptor], Context)) catch
        #'EventNotFound'{} ->
            {error, version_not_found}
    end.

-spec commit(dmt:version(), dmt:commit(), context()) ->
    dmt:version() | {error, version_not_found | operation_conflict}.
commit(Version, Commit, Context) ->
    Descriptor = prepare_descriptor(?NS, ?REF, #'HistoryRange'{}),
    Call = term_to_binary({commit, Version, Commit}),
    binary_to_term(call('Call', [Descriptor, Call], Context)).

%%

-spec call(atom(), list(term()), context()) -> term() | no_return().
call(Method, Args, Context) ->
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
                    error(Exception)
            end;
        {exception, Exception} ->
            error(Exception)
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
