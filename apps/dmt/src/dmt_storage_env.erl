-module(dmt_storage_env).
-behaviour(dmt_storage).

-export([get_head/0]).
-export([get_commit/1]).
-export([save/2]).

-define(SNAPSHOT_ENV, storage_env_snapshot).
-define(COMMITS_ENV, storage_env_commits).

-include_lib("dmt_proto/include/dmt_domain_config_thrift.hrl").

-spec get_head() -> dmt:snapshot().
get_head() ->
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
save(Commit, #'Snapshot'{version = Version} = Snapshot) ->
    ok = application:set_env(dmt, ?SNAPSHOT_ENV, Snapshot),
    Commits = application:get_env(dmt, ?COMMITS_ENV, #{}),
    ok = application:set_env(dmt, ?COMMITS_ENV, Commits#{Version => Commit}).

new_snapshot() ->
    #'Snapshot'{version = 0, domain = dmt_domain:new()}.
