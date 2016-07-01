-module(dmt_storage).

%%

-callback get_commit(dmt:version()) -> dmt:commit().
-callback get_history() -> dmt:history().
-callback commit(dmt:commit()) -> ok.

%%

-export([get_commit/1]).
-export([get_history/0]).
-export([commit/1]).

%%

-spec get_commit(dmt:version()) -> dmt:commit().
get_commit(Version) ->
    {ok, Module} = application:get_env(dmt, storage),
    Module:get_commit(Version).

-spec get_history() -> dmt:history().
get_history() ->
    {ok, Module} = application:get_env(dmt, storage),
    Module:get_history().

-spec commit(dmt:commit()) -> ok.
commit(Commit) ->
    {ok, Module} = application:get_env(dmt, storage),
    Module:commit(Commit).
