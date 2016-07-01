-module(dmt_api_mgun_handler).
-behaviour(woody_server_thrift_handler).

-export([handle_function/4]).

%%

-include_lib("dmt_proto/include/dmt_state_processing_thrift.hrl").


-spec handle_function(
    woody_t:func(),
    woody_server_thrift_handler:args(),
    woody_client:context(),
    woody_server_thrift_handler:handler_opts()
) -> {ok | {ok, woody_server_thrift_handler:result()}, woody_client:context()} | no_return().
handle_function(processCall, {#'CallArgs'{call = Call}}, Context, _Opts) ->
    CA = #'ComplexAction'{},
    {{ok, #'CallResult'{events = [Call], action = CA, response = <<"ok">>}}, Context};
handle_function(processSignal, {#'SignalArgs'{signal = {init, #'InitSignal'{}}}}, Context, _Opts) ->
    CA = #'ComplexAction'{tag = #'TagAction'{tag = <<"dmt_storage">>}},
    {{ok, #'SignalResult'{events = [], action = CA}}, Context}.
