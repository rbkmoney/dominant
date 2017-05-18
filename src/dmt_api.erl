-module(dmt_api).
-behaviour(application).
-behaviour(supervisor).

%% API
-export([checkout/3]).
-export([checkout_object/4]).
-export([pull/3]).
-export([commit/4]).
-export([apply_commit/6]).

%% behaviours
-export([start/2]).
-export([stop/1]).
-export([init/1]).

-include_lib("dmsl/include/dmsl_domain_config_thrift.hrl").

%% API
-type ref() :: dmsl_domain_config_thrift:'Reference'().
-type object_ref() :: dmsl_domain_thrift:'Reference'().
-type context() :: woody_client:context().
-type repository() :: module().

-type version() :: dmt_api_repository:version().
-type snapshot() :: dmt_api_repository:snapshot().
-type commit() :: dmt_api_repository:commit().
-type history() :: dmt_api_repository:history().

-spec checkout(ref(), repository(), context()) ->
    {ok, snapshot()} | {error, version_not_found}.

checkout({head, #'Head'{}}, Repository, Context) ->
    ClosestSnapshot = ensure_snapshot(dmt_api_cache:get_latest()),
    {ok, History} = dmt_api_repository:get_history(Repository, ClosestSnapshot#'Snapshot'.version, undefined, Context),
    Snapshot = dmt_history:head(History, ClosestSnapshot),
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

-spec try_get_snapshot(version(), repository(), context()) ->
    {ok, snapshot()} | {error, version_not_found}.

try_get_snapshot(Version, Repository, Context) ->
    ClosestSnapshot = ensure_snapshot(dmt_api_cache:get_closest(Version)),
    From = min(Version, ClosestSnapshot#'Snapshot'.version),
    Limit = abs(Version - ClosestSnapshot#'Snapshot'.version),
    case dmt_api_repository:get_history(Repository, From, Limit, Context) of
        {ok, History} when map_size(History) =:= Limit ->
            {ok, dmt_history:travel(Version, History, ClosestSnapshot)};
        {error, version_not_found} ->
            {error, version_not_found}
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

try_get_object(ObjectReference, #'Snapshot'{version = Version, domain = Domain}) ->
    case dmt_domain:get_object(ObjectReference, Domain) of
        {ok, Object} ->
            {ok, #'VersionedObject'{version = Version, object = Object}};
        error ->
            {error, object_not_found}
    end.

-spec pull(version(), repository(), context()) ->
    {ok, history()} | {error, version_not_found}.

pull(Version, Repository, Context) ->
    dmt_api_repository:get_history(Repository, Version, undefined, Context).

-spec commit(version(), commit(), repository(), context()) ->
    {ok, version()} | {error, version_not_found | operation_conflict}.

commit(Version, Commit, Repository, Context) ->
    case ensure_snapshot(dmt_api_cache:get_latest()) of
        #'Snapshot'{version = CachedVersion} when Version >= CachedVersion ->
            case dmt_api_repository:commit(Repository, Version, Commit, CachedVersion, Context) of
                {ok, Snapshot = #'Snapshot'{version = VersionNext}} ->
                    _ = dmt_api_cache:put(Snapshot),
                    {ok, VersionNext};
                {error, _} = Error ->
                    Error
            end;
        _ ->
            {error, operation_conflict}
    end.

-spec apply_commit(version(), commit(), version(), history(), repository(), context()) ->
    {ok, snapshot()} | {error, term()}.

apply_commit(VersionWas, #'Commit'{ops = Ops}, BaseVersion, History, Repository, Context) ->
    case checkout({version, BaseVersion}, Repository, Context) of
        {ok, BaseSnapshot} ->
            SnapshotWas = dmt_history:head(History, BaseSnapshot),
            try_apply_commit(VersionWas, SnapshotWas, Ops);
        {error, version_not_found} ->
            {error, version_not_found}
    end.

try_apply_commit(VersionWas, #'Snapshot'{version = VersionWas, domain = DomainWas}, Ops) ->
    try
        Domain = dmt_domain:apply_operations(Ops, DomainWas),
        {ok, #'Snapshot'{version = VersionWas + 1, domain = Domain}}
    catch
        Reason ->
            {error, Reason}
    end;

try_apply_commit(VersionWas, #'Snapshot'{version = Version}, _) when Version > VersionWas ->
    {error, {head_mismatch, Version}};

try_apply_commit(_, _, _) ->
    {error, version_not_found}.

%% behaviours
-spec start(application:start_type(), term()) -> {ok, pid()} | {error, term()}.

start(_StartType, _Args) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec stop(term()) -> ok.

stop(_State) ->
    ok.

%%

-spec init(any()) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.

init(_) ->
    {ok, IP} = inet:parse_address(genlib_app:env(?MODULE, ip, "::")),
    API = woody_server:child_spec(
        ?MODULE,
        #{
            ip            => IP,
            port          => genlib_app:env(?MODULE, port, 8022),
            net_opts      => genlib_app:env(?MODULE, net_opts, #{}),
            event_handler => woody_event_handler_default,
            handlers      => [
                get_handler_spec(repository),
                get_handler_spec(repository_client),
                get_handler_spec(state_processor)
            ]
        }
    ),
    Cache = dmt_api_cache:child_spec(),
    Children = [Cache, API],
    {ok, {#{strategy => one_for_one, intensity => 10, period => 60}, Children}}.

-spec get_handler_spec(Which) -> {Path, {woody:service(), woody:handler(module())}} when
    Which   :: repository | repository_client | state_processor,
    Path    :: iodata().

get_handler_spec(repository) ->
    {"/v1/domain/repository", {
        {dmsl_domain_config_thrift, 'Repository'},
        {dmt_api_repository_handler, get_repository_mod()}
    }};
get_handler_spec(repository_client) ->
    {"/v1/domain/repository_client", {
        {dmsl_domain_config_thrift, 'RepositoryClient'},
        {dmt_api_repository_client_handler, get_repository_mod()}
    }};
get_handler_spec(state_processor) ->
    {"/v1/stateproc", {
        {dmsl_state_processing_thrift, 'Processor'},
        get_repository_mod()
    }}.

get_repository_mod() ->
    genlib_app:env(?MODULE, repository, dmt_api_repository_v2).

ensure_snapshot({ok, Snapshot}) ->
    Snapshot;
ensure_snapshot({error, version_not_found}) ->
    #'Snapshot'{version = 0, domain = dmt_domain:new()}.
