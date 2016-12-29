-module(dmt_api_repository_handler).
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
handle_function('Commit', [Version, Commit], Context, _Opts) ->
    case dmt_api:commit(Version, Commit, Context) of
        VersionNext when is_integer(VersionNext) ->
            {ok, VersionNext};
        {error, operation_conflict} ->
            woody_error:raise(business, #'OperationConflict'{});
        {error, version_not_found} ->
            woody_error:raise(business, #'VersionNotFound'{})
    end;
handle_function('Checkout', [Reference], Context, _Opts) ->
    case dmt_api:checkout(Reference, Context) of
        Snapshot = #'Snapshot'{} ->
            {ok, Snapshot};
        {error, version_not_found} ->
            woody_error:raise(business, #'VersionNotFound'{})
    end;
handle_function('Pull', [Version], Context, _Opts) ->
    case dmt_api:pull(Version, Context) of
        History = #{} ->
            {ok, History};
        {error, version_not_found} ->
            woody_error:raise(business, #'VersionNotFound'{})
    end.
