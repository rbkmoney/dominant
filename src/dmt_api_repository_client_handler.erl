-module(dmt_api_repository_client_handler).
-behaviour(woody_server_thrift_handler).

-export([handle_function/4]).

%%

-include_lib("dmsl/include/dmsl_domain_config_thrift.hrl").

-type context() :: woody_context:ctx().

-spec handle_function
    ('checkoutObject', woody:args(), context(), woody:options()) ->
        {ok, dmsl_domain_config_thrift:'VersionedObject'()} | no_return().
handle_function('checkoutObject', [Reference, ObjectReference], Context, _Opts) ->
    case dmt_api:checkout_object(Reference, ObjectReference, Context) of
        {ok, Object} ->
            {ok, Object};
        {error, object_not_found} ->
            woody_error:raise(business, #'ObjectNotFound'{});
        {error, version_not_found} ->
            woody_error:raise(business, #'VersionNotFound'{})
    end.
