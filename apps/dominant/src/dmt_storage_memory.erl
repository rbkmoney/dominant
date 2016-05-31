-module(dmt_storage_memory).
-behaviour(dmt_storage).

%%

-export([child_spec/1]).

-export([init/1]).
-export([read/2]).
-export([write/3]).

%%

-spec child_spec(dmt_storage:options()) ->
    ignore.

child_spec(#{}) ->
    ignore.

%%

-type context() :: #{dmt_storage:key() => dmt_storage:value()}.

-spec init(dmt_storage:options()) ->
    context().

init(#{}) ->
    Context = ets:new(?MODULE, [private]),
    % TODO more general bootstrapping
    write({ref, head}, 0, Context),
    write({data, 0}, dmt_data:new(), Context),
    Context.

-spec read(dmt_storage:key(), context()) ->
    {ok, dmt_storage:value()} | {error, notfound}.

read(Key, Context) ->
    case ets:lookup(Context, Key) of
        [{Key, Value}] ->
            {ok, Value};
        [] ->
            {error, notfound}
    end.

-spec write(dmt_storage:key(), dmt_storage:value(), context()) ->
    {ok, dmt_storage:value()}.

write(Key, Value, Context) ->
    true = ets:insert(Context, {Key, Value}),
    {ok, Value}.
