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
) -> {woody_server_thrift_handler:result(), woody_client:context()} | no_return().
handle_function('Commit', {Version, Commit}, Context, _Opts) ->
    case dmt_api:commit(Version, Commit, Context) of
        {VersionNext, Context1} when is_integer(VersionNext) ->
            {VersionNext, Context1};
        {{error, operation_conflict}, Context1} ->
            throw({#'OperationConflict'{}, Context1});
        {{error, version_not_found}, Context1} ->
            throw({#'VersionNotFound'{}, Context1})
    end;
handle_function('Checkout', {Reference}, Context, _Opts) ->
    case dmt_api:checkout(Reference, Context) of
        {Snapshot = #'Snapshot'{}, Context1} ->
            {Snapshot, Context1};
        {{error, version_not_found}, Context1} ->
            throw({#'VersionNotFound'{}, Context1})
    end;
handle_function('Pull', {Version}, Context, _Opts) ->
    case dmt_api:pull(Version, Context) of
        {History = #{}, Context1} ->
            {History, Context1};
        {{error, version_not_found}, Context1} ->
            throw({#'VersionNotFound'{}, Context1})
    end.
