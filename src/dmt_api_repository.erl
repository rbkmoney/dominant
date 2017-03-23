-module(dmt_api_repository).

%% API

-export([get_history/3]).
-export([get_history_since/3]).
-export([commit/4]).

-type context() :: woody_context:ctx().

-callback get_history(pos_integer() | undefined, context()) ->
    dmt:history().
-callback get_history_since(dmt:version(), context()) ->
    {ok, dmt:history()} | {error, version_not_found}.
-callback commit(dmt:version(), dmt:commit(), context()) ->
    {ok, dmt:snapshot()} | {error, version_not_found | operation_conflict}.

-spec get_history(module(), pos_integer() | undefined, context()) ->
    dmt:history().
get_history(Mod, Limit, Context) ->
    Mod:get_history(Limit, Context).

-spec get_history_since(module(), dmt:version(), context()) ->
    {ok, dmt:history()} | {error, version_not_found}.
get_history_since(Mod, Version, Context) ->
    Mod:get_history_since(Version, Context).

-spec commit(module(), dmt:version(), dmt:commit(), context()) ->
    {ok, dmt:snapshot()} | {error, version_not_found | operation_conflict}.
commit(Mod, Version, Commit, Context) ->
    Mod:commit(Version, Commit, Context).
