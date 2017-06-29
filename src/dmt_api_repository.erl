-module(dmt_api_repository).

-include_lib("dmsl/include/dmsl_domain_config_thrift.hrl").

%% API

-export([checkout/3]).
-export([checkout_object/4]).
-export([pull/3]).
-export([commit/4]).
-export([apply_commit/5]).

-export_type([version/0]).
-export_type([snapshot/0]).
-export_type([commit/0]).
-export_type([history/0]).
-export_type([operation_conflict/0]).

-type version() :: dmsl_domain_config_thrift:'Version'().
-type snapshot() :: dmsl_domain_config_thrift:'Snapshot'().
-type commit() :: dmsl_domain_config_thrift:'Commit'().
-type history() :: dmsl_domain_config_thrift:'History'().

-type ref() :: dmsl_domain_config_thrift:'Reference'().
-type object_ref() :: dmsl_domain_thrift:'Reference'().
-type repository() :: module().
-type context() :: woody_context:ctx().

-type operation_conflict() ::
    {object_already_exists, object_ref()} |
    {object_not_found, object_ref()} |
    {object_reference_mismatch, object_ref()} |
    {objects_not_exist, [{object_ref(), [object_ref()]}]}.

-callback get_history(pos_integer() | undefined, context()) ->
    history().
-callback get_history(version(), pos_integer() | undefined, context()) ->
    {ok, history()} | {error, version_not_found}.
-callback commit(version(), commit(), context()) ->
    {ok, snapshot()} |
    {error, version_not_found | {operation_conflict, operation_conflict()}}.

%%

-spec checkout(ref(), repository(), context()) ->
    {ok, snapshot()} | {error, version_not_found}.

checkout({head, #'Head'{}}, Repository, Context) ->
    ClosestSnapshot = ensure_snapshot(dmt_api_cache:get_latest()),
    {ok, History} = get_history(Repository, ClosestSnapshot#'Snapshot'.version, undefined, Context),
    %% TO DO: Need to fix dmt_history:head. It can return {error, ...}
    {ok, Snapshot} = dmt_history:head(History, ClosestSnapshot),
    {ok, dmt_api_cache:put(Snapshot)};

checkout({version, Version}, Repository, Context) ->
    case dmt_api_cache:get(Version) of
        {ok, Snapshot} ->
            {ok, Snapshot};
        {error, version_not_found} ->
            case try_get_snapshot(Version, Repository, Context) of
                {ok, Snapshot} ->
                    {ok, dmt_api_cache:put(Snapshot)};
                {error, version_not_found} ->
                    {error, version_not_found}
            end
    end.

-spec checkout_object(ref(), object_ref(), repository(), context()) ->
    {ok, dmsl_domain_config_thrift:'VersionedObject'()} | {error, version_not_found | object_not_found}.

checkout_object(Reference, ObjectReference, Repository, Context) ->
    case checkout(Reference, Repository, Context) of
        {ok, Snapshot} ->
            try_get_object(ObjectReference, Snapshot);
        {error, _} = Error ->
            Error
    end.

-spec pull(version(), repository(), context()) ->
    {ok, history()} | {error, version_not_found}.

pull(Version, Repository, Context) ->
    get_history(Repository, Version, undefined, Context).

-spec commit(version(), commit(), repository(), context()) ->
    {ok, version()} |
    {error, version_not_found | head_mismatch | {operation_conflict, operation_conflict()}}.

commit(Version, Commit, Repository, Context) ->
    case ensure_snapshot(dmt_api_cache:get_latest()) of
        #'Snapshot'{version = CachedVersion} when Version >= CachedVersion ->
            case Repository:commit(Version, Commit, Context) of
                {ok, Snapshot = #'Snapshot'{version = VersionNext}} ->
                    _ = dmt_api_cache:put(Snapshot),
                    {ok, VersionNext};
                {error, _} = Error ->
                    Error
            end;
        _ ->
            {error, head_mismatch}
    end.

-spec apply_commit(version(), commit(), history(), repository(), context()) ->
    {ok, snapshot()} | {error, term()}.

% FIXME map_size(History) should be in dmt_history.
apply_commit(VersionWas, #'Commit'{ops = Ops}, History, Repository, Context) when map_size(History) =:= 0 ->
    case checkout({version, VersionWas}, Repository, Context) of
        {ok, BaseSnapshot} ->
            try_apply_commit(BaseSnapshot, Ops);
        {error, version_not_found} ->
            {error, version_not_found}
    end;

apply_commit(_, _, _, _, _) ->
    {error, version_not_found}. % FIXME move to dmt_history

%% Internal

-spec try_get_snapshot(version(), repository(), context()) ->
    {ok, snapshot()} | {error, version_not_found}.

try_get_snapshot(Version, Repository, Context) ->
    ClosestSnapshot = ensure_snapshot(dmt_api_cache:get_closest(Version)),
    From = min(Version, ClosestSnapshot#'Snapshot'.version),
    Limit = abs(Version - ClosestSnapshot#'Snapshot'.version),
    case get_history(Repository, From, Limit, Context) of
        {ok, History} when map_size(History) =:= Limit ->
            %% TO DO: Need to fix dmt_history:travel. It can return {error, ...}
            {ok, Snapshot} = dmt_history:travel(Version, History, ClosestSnapshot),
            {ok, Snapshot};
        {error, version_not_found} ->
            {error, version_not_found}
    end.

-spec try_get_object(object_ref(), snapshot()) ->
    {ok, dmsl_domain_config_thrift:'VersionedObject'()} | {error, object_not_found}.

try_get_object(ObjectReference, #'Snapshot'{version = Version, domain = Domain}) ->
    case dmt_domain:get_object(ObjectReference, Domain) of
        {ok, Object} ->
            {ok, #'VersionedObject'{version = Version, object = Object}};
        error ->
            {error, object_not_found}
    end.

try_apply_commit(#'Snapshot'{version = VersionWas, domain = DomainWas}, Ops) ->
    case dmt_domain:apply_operations(Ops, DomainWas) of
        {ok, Domain} ->
            {ok, #'Snapshot'{version = VersionWas + 1, domain = Domain}};
        {error, _} = Error ->
            Error
    end.

-spec ensure_snapshot({ok, snapshot()} | {error, version_not_found}) ->
    snapshot().

ensure_snapshot({ok, Snapshot}) ->
    Snapshot;
ensure_snapshot({error, version_not_found}) ->
    #'Snapshot'{version = 0, domain = dmt_domain:new()}.

-spec get_history(repository(), version(), pos_integer() | undefined, context()) ->
    {ok, history()} | {error, version_not_found}.

get_history(Mod, Version, Limit, Context) ->
    Mod:get_history(Version, Limit, Context).
