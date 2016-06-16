-module(dmt_storage).

%%

-callback get_snapshot() -> dmt:snapshot().
-callback get_commit(dmt:version()) -> dmt:commit().
-callback save(dmt:commit(), dmt:snapshot()) -> ok.

%%

-export([get_snapshot/0]).
-export([get_commit/1]).
-export([save/2]).

%%

-spec get_snapshot() -> dmt:snapshot().
get_snapshot() ->
    {ok, Module} = application:get_env(dmt, storage),
    Module:get_snapshot().

-spec get_commit(dmt:version()) -> dmt:commit().
get_commit(Version) ->
    {ok, Module} = application:get_env(dmt, storage),
    Module:get_commit(Version).

-spec save(dmt:commit(), dmt:snapshot()) -> ok.
save(Commit, Snapshot) ->
    {ok, Module} = application:get_env(dmt, storage),
    Module:save(Commit, Snapshot).