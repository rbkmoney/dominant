-module(dmt_storage_env).
-behaviour(dmt_storage).

-export([get_commit/1]).
-export([get_history/0]).
-export([commit/1]).

-define(STORAGE_ENV, storage_env).

-include_lib("dmt_proto/include/dmt_domain_config_thrift.hrl").

-spec get_commit(dmt:version()) -> dmt:commit().
get_commit(Version) ->
    case application:get_env(dmt, ?STORAGE_ENV, #{}) of
        #{Version := Commit} ->
            Commit;
        #{} ->
            throw(not_found)
    end.
-spec get_history() -> dmt:history().
get_history() ->
    application:get_env(dmt, ?STORAGE_ENV, #{}).

-spec commit(dmt:commit()) -> ok.
commit(Commit) ->
    Commits = application:get_env(dmt, ?STORAGE_ENV, #{}),
    LastCommit =  case maps:keys(Commits) of
        [] ->
            0;
        Keys ->
            lists:max(Keys)
    end,
    ok = application:set_env(dmt, ?STORAGE_ENV, Commits#{LastCommit + 1 => Commit}).
