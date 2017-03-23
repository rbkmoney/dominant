-module(dmt_api).
-behaviour(application).
-behaviour(supervisor).

%% API
-export([checkout/3]).
-export([checkout_object/4]).
-export([pull/3]).
-export([commit/4]).
-export([apply_commit/3]).

%% behaviours
-export([start/2]).
-export([stop/1]).
-export([init/1]).

-include_lib("dmsl/include/dmsl_domain_config_thrift.hrl").

%% API

-type context() :: woody_client:context().
-type repository() :: module().

-spec checkout(dmt:ref(), repository(), context()) ->
    {ok, dmt:snapshot()} | {error, version_not_found}.
checkout(Reference, Repository, Context) ->
    try
        {ok, dmt_cache:checkout(Reference)}
    catch
        version_not_found ->
            case try_get_snapshot(Reference, Repository, Context) of
                {ok, Snapshot} ->
                    {ok, dmt_cache:cache_snapshot(Snapshot)};
                {error, version_not_found} ->
                    {error, version_not_found}
            end
    end.

-spec try_get_snapshot(dmt:ref(), repository(), context()) ->
    {ok, dmt:snapshot()} | {error, version_not_found}.
try_get_snapshot(Reference, Repository, Context) ->
    History = dmt_api_repository:get_history(Repository, reference_to_limit(Reference), Context),
    case {Reference, dmt_history:head(History)} of
        {{head, #'Head'{}}, Snapshot} ->
            {ok, Snapshot};
        {{version, V}, Snapshot = #'Snapshot'{version = V}} ->
            {ok, Snapshot};
        {{version, V1}, #'Snapshot'{version = V2}} when V1 > V2 ->
            {error, version_not_found}
    end.

-spec reference_to_limit(dmt:ref()) -> pos_integer() | undefined.
reference_to_limit({head, #'Head'{}}) ->
    undefined;
reference_to_limit({version, Version}) ->
    Version.

-spec checkout_object(dmt:ref(), dmt:object_ref(), repository(), context()) ->
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

-spec pull(dmt:version(), repository(), context()) ->
    {ok, dmt:history()} | {error, version_not_found}.
pull(Version, Repository, Context) ->
    dmt_api_repository:get_history_since(Repository, Version, Context).

-spec commit(dmt:version(), dmt:commit(), repository(), context()) ->
    {ok, dmt:version()} | {error, version_not_found | operation_conflict}.
commit(Version, Commit, Repository, Context) ->
    case dmt_api_repository:commit(Repository, Version, Commit, Context) of
        {ok, Snapshot = #'Snapshot'{version = VersionNext}} ->
            _ = dmt_cache:cache_snapshot(Snapshot),
            {ok, VersionNext};
        {error, _} = Error ->
            Error
    end.

-spec apply_commit(dmt:version(), dmt:commit(), dmt:history()) ->
    {ok, dmt:snapshot()} | {error, term()}.
apply_commit(VersionWas, #'Commit'{ops = Ops}, History) ->
    SnapshotWas = dmt_history:head(History),
    case SnapshotWas of
        #'Snapshot'{version = VersionWas, domain = DomainWas} ->
            try
                Domain = dmt_domain:apply_operations(Ops, DomainWas),
                {ok, #'Snapshot'{version = VersionWas + 1, domain = Domain}}
            catch
                Reason ->
                    {error, Reason}
            end;
        #'Snapshot'{version = Version} when Version > VersionWas ->
            {error, {head_mismatch, Version}};
        #'Snapshot'{} ->
            {error, version_not_found}
    end.

%% behaviours
-spec start(application:start_type(), term()) -> {ok, pid()} | {error, term()}.

start(_StartType, _Args) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec stop(term()) -> ok.

stop(_State) ->
    ok.

%%

init([]) ->
    {ok, IP} = inet:parse_address(genlib_app:env(?MODULE, ip, "::")),
    API = woody_server:child_spec(
        ?MODULE,
        #{
            ip            => IP,
            port          => genlib_app:env(?MODULE, port, 8022),
            event_handler => woody_event_handler_default,
            handlers      => [
                get_handler_spec(repository),
                get_handler_spec(repository_client),
                get_handler_spec(state_processor)
            ]
        }
    ),
    Children = [API],
    {ok, {#{strategy => one_for_one, intensity => 10, period => 60}, Children}}.

-spec get_handler_spec(Which) -> {Path, {woody:service(), module()}} when
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
