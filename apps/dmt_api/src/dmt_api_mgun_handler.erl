-module(dmt_api_mgun_handler).
-behaviour(woody_server_thrift_handler).

-export([handle_function/4]).

%%

-include_lib("dmt_proto/include/dmt_state_processing_thrift.hrl").
-include_lib("dmt/include/dmt_mg.hrl").


-spec handle_function(
    woody_t:func(),
    woody_server_thrift_handler:args(),
    woody_client:context(),
    woody_server_thrift_handler:handler_opts()
) -> {ok | {ok, woody_server_thrift_handler:result()}, woody_client:context()} | no_return().
handle_function(processCall, {#'CallArgs'{call = <<"commit", Data/binary>>, history = History}}, Context, _Opts) ->
    {Version, Commit} = binary_to_term(Data),
    ok = dmt:validate_commit(Version, Commit, dmt_mg:read_history(History)),
    _Snapshot = dmt_cache:commit(Commit),
    {
        {ok, #'CallResult'{
            events = [term_to_binary(Commit)],
            action = #'ComplexAction'{},
            response = <<"ok">>
        }},
        Context
    };
handle_function(processSignal, {#'SignalArgs'{signal = {init, #'InitSignal'{}}}}, Context, _Opts) ->
    CA = #'ComplexAction'{tag = #'TagAction'{tag = ?MG_TAG}},
    {{ok, #'SignalResult'{events = [], action = CA}}, Context}.
