-module(dmt_storage_env).
-behaviour(dmt_storage).

-export([get_snapshot/0]).
-export([get_commit/1]).
-export([save/2]).

-define(SNAPSHOT_ENV, storage_env_snapshot).
-define(COMMITS_ENV, storage_env_commits).

-spec get_snapshot() -> dmt:snapshot().
get_snapshot() ->
    application:get_env(dmt, ?SNAPSHOT_ENV, new_snapshot()).

-spec get_commit(dmt:version()) -> dmt:commit().
get_commit(Version) ->
    case application:get_env(dmt, ?COMMITS_ENV, #{}) of
        #{Version := Commit} ->
            Commit;
        #{} ->
            throw(not_found)
    end.

-spec save(dmt:commit(), dmt:snapshot()) -> ok.
save(Commit, {Version, _Schema, _Data} = Snapshot) ->
    ok = application:set_env(dmt, ?SNAPSHOT_ENV, Snapshot),
    Commits = application:get_env(dmt, ?COMMITS_ENV, #{}),
    ok = application:set_env(dmt, ?COMMITS_ENV, Commits#{Version => Commit}).

new_snapshot() ->
    {0, dmt:current_schema(), dmt_data:new()}.