-module(dmt_api_repository_client_handler).
-behaviour(woody_server_thrift_handler).

-include_lib("damsel/include/dmsl_domain_config_thrift.hrl").

-export([handle_function/4]).

-type options() :: #{
    repository := module(),
    default_handling_timeout := timeout()
}.

-export_type([options/0]).

%% API

-type context() :: woody_context:ctx().

-spec handle_function
    ('checkoutObject', woody:args(), context(), options()) ->
        {ok, dmsl_domain_config_thrift:'VersionedObject'()} | no_return().
handle_function('checkoutObject', {Reference, ObjectReference}, Context0, Options) ->
    DefaultDeadline = woody_deadline:from_timeout(default_handling_timeout(Options)),
    Context = dmt_api_woody_utils:ensure_woody_deadline_set(Context0, DefaultDeadline),
    case dmt_api_repository:checkout_object(Reference, ObjectReference, repository(Options), Context) of
        {ok, Object} ->
            {ok, Object};
        {error, object_not_found} ->
            woody_error:raise(business, #'ObjectNotFound'{});
        {error, version_not_found} ->
            woody_error:raise(business, #'VersionNotFound'{})
    end.

%% Internals

-spec repository(options()) ->
    module().
repository(#{repository := Repository}) ->
    Repository.

-spec default_handling_timeout(options()) ->
    timeout().
default_handling_timeout(#{default_handling_timeout := Timeout}) ->
    Timeout.
