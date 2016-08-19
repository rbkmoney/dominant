-module(dmt_api_repository_client_handler).
-behaviour(woody_server_thrift_handler).

-export([handle_function/4]).

%%

-include_lib("dmt/include/dmt_domain_config_thrift.hrl").

-spec handle_function(
    woody_t:func(),
    woody_server_thrift_handler:args(),
    woody_client:context(),
    woody_server_thrift_handler:handler_opts()
) -> {ok | {ok, woody_server_thrift_handler:result()}, woody_client:context()} | no_return().
handle_function('checkoutObject', {Reference, ObjectReference}, Context, _Opts) ->
    try
        Object = dmt_api:checkout_object(Reference, ObjectReference),
        {{ok, Object}, Context}
    catch
        object_not_found ->
            throw({#'ObjectNotFound'{}, Context});
        version_not_found ->
            throw({#'VersionNotFound'{}, Context})
    end.
