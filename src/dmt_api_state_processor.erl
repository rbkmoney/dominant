-module(dmt_api_state_processor).
-behaviour(woody_server_thrift_handler).

-export([handle_function/4]).

%%

-include("dmt_api_state_processing_thrift.hrl").


-spec handle_function(
    woody_t:func(),
    woody_server_thrift_handler:args(),
    woody_client:context(),
    woody_server_thrift_handler:handler_opts()
) -> {ok | {ok, woody_server_thrift_handler:result()}, woody_client:context()} | no_return().
handle_function('ProcessCall', {#'CallArgs'{arg = Payload, history = History}}, Context, _Opts) ->
    {Response, Events} = handle_call(binary_to_term(Payload), dmt_api_mg:read_history(History)),
    {
        #'CallResult'{
            events = lists:map(fun term_to_binary/1, Events),
            action = #'ComplexAction'{},
            response = term_to_binary(Response)
        },
        Context
    };
handle_function('ProcessSignal', {#'SignalArgs'{signal = {init, #'InitSignal'{}}}}, Context, _Opts) ->
    {#'SignalResult'{events = [], action = #'ComplexAction'{}}, Context}.

%%

handle_call({commit, Version, Commit}, History) ->
    case dmt_api:apply_commit(Version, Commit, History) of
        {ok, Snapshot} ->
            {Snapshot, [Commit]};
        {error, Reason} ->
            lager:info("commit failed: ~p", [Reason]),
            {{error, operation_conflict}, []}
    end.
