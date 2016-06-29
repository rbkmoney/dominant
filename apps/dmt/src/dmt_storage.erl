-module(dmt_storage).

%%

-callback get_head() -> dmt:snapshot().
-callback get_commit(dmt:version()) -> dmt:commit().
-callback save(dmt:commit(), dmt:snapshot()) -> ok.

%%

-export([get_head/0]).
-export([get_commit/1]).
-export([save/2]).

%%

-spec get_head() -> dmt:snapshot().
get_head() ->
    {ok, Module} = application:get_env(dmt, storage),
    Module:get_head().

-spec get_commit(dmt:version()) -> dmt:commit().
get_commit(Version) ->
    {ok, Module} = application:get_env(dmt, storage),
    Module:get_commit(Version).

-spec save(dmt:commit(), dmt:snapshot()) -> ok.
save(Commit, Snapshot) ->
    {ok, Module} = application:get_env(dmt, storage),
    Module:save(Commit, Snapshot).