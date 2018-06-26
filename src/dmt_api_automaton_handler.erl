-module(dmt_api_automaton_handler).

-include_lib("mg_proto/include/mg_proto_state_processing_thrift.hrl").

-callback process_call(call(), machine(), context()) ->
    {response(), events()} | no_return().

-callback process_signal(signal(), machine(), context()) ->
    {action(), events()} | no_return().

-export_type([call/0]).
-export_type([signal/0]).
-export_type([machine/0]).
-export_type([response/0]).
-export_type([action/0]).
-export_type([events/0]).

-type call()        :: mg_proto_state_processing_thrift:'Args'().
-type signal()      :: mg_proto_state_processing_thrift:'Signal'().
-type machine()     :: mg_proto_state_processing_thrift:'Machine'().
-type response()    :: mg_proto_state_processing_thrift:'CallResponse'().
-type action()      :: mg_proto_state_processing_thrift:'ComplexAction'().
-type events()      :: mg_proto_state_processing_thrift:'EventBodies'().
-type context()     :: woody_context:ctx().

%% State processor

-behaviour(woody_server_thrift_handler).

-export([handle_function/4]).

-define(NIL, {nl, #mg_msgpack_Nil{}}).

-spec handle_function(woody:func(), woody:args(), context(), woody:options()) ->
    {ok, woody:result()} | no_return().

handle_function('ProcessCall', [#mg_stateproc_CallArgs{arg = Payload, machine = Machine}], Context, _Opts) ->
    Handler = get_handler(Machine),
    {Response, Events} = Handler:process_call(Payload, Machine, Context),
    {ok, construct_call_result(Response, Events)};

handle_function('ProcessSignal', [#mg_stateproc_SignalArgs{signal = Signal, machine = Machine}], Context, _Opts) ->
    Handler = get_handler(Machine),
    {Action, Events} = Handler:process_signal(Signal, Machine, Context),
    {ok, construct_signal_result(Action, Events)}.

%% Internals

construct_call_result(Response, Events) ->
    #mg_stateproc_CallResult{
        response = Response,
        change = #mg_stateproc_MachineStateChange{aux_state = ?NIL, events = Events},
        action = #mg_stateproc_ComplexAction{}
    }.

construct_signal_result(Action, Events) ->
    #mg_stateproc_SignalResult{
        change = #mg_stateproc_MachineStateChange{aux_state = ?NIL, events = Events},
        action = Action
    }.

%% FIXME OH MY GOSH! What an ugly hack?!
get_handler(#mg_stateproc_Machine{id = MachineID}) ->
    case MachineID of
        <<"primary/v3">> ->
            dmt_api_repository_v3;
        <<"primary/v4">> ->
            dmt_api_repository_v4
    end.
