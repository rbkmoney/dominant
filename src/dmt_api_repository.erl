-module(dmt_api_repository).

-include_lib("dmsl/include/dmsl_domain_config_thrift.hrl").

%% API

-export([checkout/3]).
-export([checkout_object/4]).
-export([pull/3]).
-export([commit/4]).

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

-callback checkout(ref(), context()) ->
    % TODO this was made due to dialyzer warning, can't find the way to fix it
    {ok, term()} |
    {error, version_not_found}.
-callback pull(version(), context()) ->
    {ok, history()} |
    {error, version_not_found}.
-callback commit(version(), commit(), context()) ->
    {ok, snapshot()} |
    {error, version_not_found | {operation_conflict, operation_conflict()}}.

%%

-spec checkout(ref(), repository(), context()) ->
    {ok, snapshot()} | {error, version_not_found}.

checkout({head, #'Head'{}} = V, Repository, Context) ->
    case Repository:checkout(V, Context) of
        {ok, Snapshot} ->
            {ok, dmt_api_cache:put(Snapshot)};
        {error, version_not_found} ->
            {error, version_not_found}
    end;

checkout({version, Version} = V, Repository, Context) ->
    case dmt_api_cache:get(Version) of
        {ok, Snapshot} ->
            {ok, Snapshot};
        {error, version_not_found} ->
            case Repository:checkout(V, Context) of
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
    Repository:pull(Version, Context).

-spec commit(version(), commit(), repository(), context()) ->
    {ok, version()} |
    {error, version_not_found | head_mismatch | {operation_conflict, operation_conflict()}}.

commit(Version, Commit, Repository, Context) ->
    case ensure_snapshot(dmt_api_cache:get_latest()) of
        #'Snapshot'{version = CachedVersion} when Version >= CachedVersion ->
            case Repository:commit(Version, Commit, Context) of
                {ok, Snapshot} ->
                    #'Snapshot'{version = VersionNext} = dmt_api_cache:put(Snapshot),
                    {ok, VersionNext};
                {error, _} = Error ->
                    Error
            end;
        _ ->
            {error, head_mismatch}
    end.

%% Internal

-spec try_get_object(object_ref(), snapshot()) ->
    {ok, dmsl_domain_config_thrift:'VersionedObject'()} | {error, object_not_found}.

try_get_object(ObjectReference, #'Snapshot'{version = Version, domain = Domain}) ->
    case dmt_domain:get_object(ObjectReference, Domain) of
        {ok, Object} ->
            {ok, #'VersionedObject'{version = Version, object = Object}};
        error ->
            {error, object_not_found}
    end.

-spec ensure_snapshot({ok, snapshot()} | {error, version_not_found}) ->
    snapshot().

ensure_snapshot({ok, Snapshot}) ->
    Snapshot;
ensure_snapshot({error, version_not_found}) ->
    #'Snapshot'{version = 0, domain = dmt_domain:new()}.
