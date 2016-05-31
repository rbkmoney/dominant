-module(dmt_api_configurator_handler).
-behaviour(woody_server_thrift_handler).
-behaviour(woody_event_handler).

-export([handle_function/4, handle_error/4]).
-export([handle_event/3]).

%%

-include_lib("dmt_proto/include/dmt_domain_config_thrift.hrl").

-spec handle_function(woody_t:func(), woody_server_thrift_handler:args(), woody_client:context(), _) ->
    {ok, term()} | no_return().

handle_function(head, {}, _Context, _Opts) ->
    {ok, #'Version'{data = {rev, 0}}};

handle_function(_Function, _, _Context, _Opts) ->
    error(noimpl).

-spec handle_error(woody_t:func(), term(), woody_client:context(), _) ->
    _.

handle_error(_Function, _Reason, _Context, _Opts) ->
    ok.

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
