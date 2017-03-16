-module(dmt_api_repository_handler).
-behaviour(woody_server_thrift_handler).

-export([handle_function/4]).

%%

-include_lib("dmsl/include/dmsl_domain_config_thrift.hrl").

-type context() :: woody_context:ctx().

-spec handle_function
    ('Commit', woody:args(), context(), woody:options()) ->
        {ok, dmt:version()} | no_return();
    ('Checkout', woody:args(), context(), woody:options()) ->
        {ok, dmt:snapshot()} | no_return();
    ('Pull', woody:args(), context(), woody:options()) ->
        {ok, dmt:history()} | no_return().
handle_function('Commit', [Version, Commit], Context, _Opts) ->
    case dmt_api:commit(Version, Commit, Context) of
        {ok, VersionNext} ->
            {ok, VersionNext};
        {error, operation_conflict} ->
            woody_error:raise(business, #'OperationConflict'{});
        {error, version_not_found} ->
            woody_error:raise(business, #'VersionNotFound'{})
    end;
handle_function('Checkout', [Reference], Context, _Opts) ->
    case dmt_api:checkout(Reference, Context) of
        {ok, Snapshot} ->
            {ok, Snapshot};
        {error, version_not_found} ->
            woody_error:raise(business, #'VersionNotFound'{})
    end;
handle_function('Pull', [Version], Context, _Opts) ->
    case dmt_api:pull(Version, Context) of
        {ok, History} ->
            {ok, History};
        {error, version_not_found} ->
            woody_error:raise(business, #'VersionNotFound'{})
    end.
