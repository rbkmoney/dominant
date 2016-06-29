-module(dmt_api_repository_handler).
-behaviour(woody_server_thrift_handler).
-behaviour(woody_event_handler).

-export([handle_function/4]).
-export([handle_event/3]).

%%

-include_lib("dmt_proto/include/dmt_domain_config_thrift.hrl").

-spec handle_function(
    woody_t:func(),
    woody_server_thrift_handler:args(),
    woody_client:context(),
    woody_server_thrift_handler:handler_opts()
) -> {ok | {ok, woody_server_thrift_handler:result()}, woody_client:context()} | no_return().
handle_function(commit, {Version, Commit}, Context, _Opts) ->
    try
        NewVersion = dmt:commit(Version, Commit),
        {{ok, NewVersion}, Context}
    catch
        operation_conflict ->
            throw({#'OperationConflict'{}, Context});
        version_not_found ->
            throw({#'VersionNotFound'{}, Context})
    end;
handle_function(checkout, {Reference}, Context, _Opts) ->
    try
        Snapshot = dmt:checkout(Reference),
        {{ok, Snapshot}, Context}
    catch
        version_not_found ->
            throw({#'VersionNotFound'{}, Context})
    end;
handle_function(pull, {Version}, Context, _Opts) ->
    try
        History = dmt:pull(Version),
        {{ok, History}, Context}
    catch
        version_not_found ->
            throw({#'VersionNotFound'{}, Context})
    end.

%%

-spec handle_event(EventType, RpcID, EventMeta)
    -> _ when
        EventType :: woody_event_handler:event_type(),
        RpcID ::  woody_t:rpc_id(),
        EventMeta :: woody_event_handler:event_meta_type().

handle_event(EventType, RpcID, #{status := error, class := Class, reason := Reason, stack := Stack}) ->
    lager:error(
        maps:to_list(RpcID),
        "[server] ~s with ~s:~p at ~s",
        [EventType, Class, Reason, genlib_format:format_stacktrace(Stack, [newlines])]
    );

handle_event(EventType, RpcID, EventMeta) ->
    lager:debug(maps:to_list(RpcID), "[server] ~s: ~p", [EventType, EventMeta]).

