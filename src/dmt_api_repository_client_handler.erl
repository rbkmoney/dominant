-module(dmt_api_repository_client_handler).
-behaviour(woody_server_thrift_handler).

-export([handle_function/4]).

%%

-include_lib("dmsl/include/dmsl_domain_config_thrift.hrl").

-spec handle_function(
    woody:func(),
    woody_server_thrift_handler:args(),
    woody_client:context(),
    woody_server_thrift_handler:handler_opts()
) -> {ok, woody_server_thrift_handler:result()} | no_return().
handle_function('checkoutObject', [Reference, ObjectReference], Context, _Opts) ->
    case dmt_api:checkout_object(Reference, ObjectReference, Context) of
        Object = #'VersionedObject'{} ->
            {ok, Object};
        {error, object_not_found} ->
            woody_error:raise(business, #'ObjectNotFound'{});
        {error, version_not_found} ->
            woody_error:raise(business, #'VersionNotFound'{})
    end.
