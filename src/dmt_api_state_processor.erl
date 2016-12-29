-module(dmt_api_state_processor).
-behaviour(woody_server_thrift_handler).

-export([handle_function/4]).

%%

-include_lib("dmsl/include/dmsl_state_processing_thrift.hrl").


-spec handle_function(
    woody:func(),
    woody_server_thrift_handler:args(),
    woody_client:context(),
    woody_server_thrift_handler:handler_opts()
) -> {ok, woody_server_thrift_handler:result()} | no_return().
handle_function('ProcessCall', [#'CallArgs'{arg = Payload, machine = Machine}], _Context, _Opts) ->
    {Response, Events} = handle_call(binary_to_term(Payload), dmt_api_mg:read_history(Machine)),
    {ok, #'CallResult'{
        change = #'MachineStateChange'{
            aux_state = <<>>,
            events = lists:map(fun term_to_binary/1, Events)
        },
        action = #'ComplexAction'{},
        response = term_to_binary(Response)
    }};
handle_function('ProcessSignal', [#'SignalArgs'{signal = {init, #'InitSignal'{}}}], _Context, _Opts) ->
    {ok, #'SignalResult'{
        change = #'MachineStateChange'{
            aux_state = <<>>,
            events = []
        },
        action = #'ComplexAction'{}
    }};
handle_function('ProcessSignal', [#'SignalArgs'{signal = {repair, #'RepairSignal'{}}}], _Context, _Opts) ->
    {ok, #'SignalResult'{
        change = #'MachineStateChange'{
            aux_state = <<>>,
            events = []
        },
        action = #'ComplexAction'{}
    }}.

%%

handle_call({commit, Version, Commit}, History) ->
    case dmt_api:apply_commit(Version, Commit, History) of
        {ok, Snapshot} ->
            {Snapshot, [Commit]};
        {error, version_not_found} ->
            {{error, version_not_found}, []};
        {error, Reason} ->
            _ = lager:info("commit failed: ~p", [Reason]),
            {{error, operation_conflict}, []}
    end.
