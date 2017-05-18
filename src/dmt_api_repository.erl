-module(dmt_api_repository).

%% API

-export([get_history/3]).
-export([get_history/4]).
-export([commit/5]).

-export_type([version/0]).
-export_type([snapshot/0]).
-export_type([commit/0]).
-export_type([history/0]).

-type version() :: dmsl_domain_config_thrift:'Version'().
-type snapshot() :: dmsl_domain_config_thrift:'Snapshot'().
-type commit() :: dmsl_domain_config_thrift:'Commit'().
-type history() :: dmsl_domain_config_thrift:'History'().

-type context() :: woody_context:ctx().

-callback get_history(pos_integer() | undefined, context()) ->
    history().
-callback get_history(version(), pos_integer() | undefined, context()) ->
    {ok, history()} | {error, version_not_found}.
-callback commit(version(), commit(), version(), context()) ->
    {ok, snapshot()} | {error, version_not_found | operation_conflict}.

-spec get_history(module(), pos_integer() | undefined, context()) ->
    history().
get_history(Mod, Limit, Context) ->
    Mod:get_history(Limit, Context).

-spec get_history(module(), version(), pos_integer() | undefined, context()) ->
    {ok, history()} | {error, version_not_found}.
get_history(Mod, Version, Limit, Context) ->
    Mod:get_history(Version, Limit, Context).

-spec commit(module(), version(), commit(), version(), context()) ->
    {ok, snapshot()} | {error, version_not_found | operation_conflict}.
commit(Mod, Version, Commit, BaseVersion, Context) ->
    Mod:commit(Version, Commit, BaseVersion, Context).
