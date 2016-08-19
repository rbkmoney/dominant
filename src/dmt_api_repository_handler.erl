-module(dmt_api_repository_handler).
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
handle_function('Commit', {Version, Commit}, Context, _Opts) ->
    try
        NewVersion = dmt_api:commit(Version, Commit),
        {{ok, NewVersion}, Context}
    catch
        operation_conflict ->
            throw({#'OperationConflict'{}, Context});
        version_not_found ->
            throw({#'VersionNotFound'{}, Context})
    end;
handle_function('Checkout', {Reference}, Context, _Opts) ->
    try
        Snapshot = dmt_api:checkout(Reference),
        {{ok, Snapshot}, Context}
    catch
        version_not_found ->
            throw({#'VersionNotFound'{}, Context})
    end;
handle_function('Pull', {Version}, Context, _Opts) ->
    try
        History = dmt_api:pull(Version),
        {{ok, History}, Context}
    catch
        version_not_found ->
            throw({#'VersionNotFound'{}, Context})
    end.
