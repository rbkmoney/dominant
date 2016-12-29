-module(dmt_api).
-behaviour(application).
-behaviour(supervisor).

%% API
-export([checkout/2]).
-export([checkout_object/3]).
-export([pull/2]).
-export([commit/3]).
-export([apply_commit/3]).

%% behaviours
-export([start/2]).
-export([stop/1]).
-export([init/1]).

-include_lib("dmsl/include/dmsl_domain_config_thrift.hrl").

%% API

-type context() :: woody_client:context().

-spec checkout(dmt:ref(), context()) -> dmt:snapshot() | {error, version_not_found}.
checkout(Reference, Context) ->
    try
        dmt_cache:checkout(Reference)
    catch
        version_not_found ->
            case try_get_snapshot(Reference, Context) of
                Snapshot = #'Snapshot'{} ->
                    dmt_cache:cache_snapshot(Snapshot);
                {error, version_not_found} ->
                    {error, version_not_found}
            end
    end.

-spec try_get_snapshot(dmt:ref(), context()) -> dmt:snapshot() | {error, version_not_found}.
try_get_snapshot(Reference, Context) ->
    History = dmt_api_mg:get_history(undefined, reference_to_limit(Reference), Context),
    case {Reference, dmt_history:head(History)} of
        {{head, #'Head'{}}, Snapshot} ->
            Snapshot;
        {{version, V}, Snapshot = #'Snapshot'{version = V}} ->
            Snapshot;
        {{version, V1}, #'Snapshot'{version = V2}} when V1 > V2 ->
            {error, version_not_found}
    end.

-spec reference_to_limit(dmt:ref()) -> pos_integer() | undefined.
reference_to_limit({head, #'Head'{}}) ->
    undefined;
reference_to_limit({version, Version}) ->
    Version.

-spec checkout_object(dmt:ref(), dmt:object_ref(), context()) ->
    dmsl_domain_config_thrift:'VersionedObject'() | {error, version_not_found | object_not_found}.
checkout_object(Reference, ObjectReference, Context) ->
    Snapshot = checkout(Reference, Context),
    case Snapshot of
        #'Snapshot'{} ->
            try_get_object(ObjectReference, Snapshot);
        {error, _} ->
            Snapshot
    end.

try_get_object(ObjectReference, #'Snapshot'{version = Version, domain = Domain}) ->
    case dmt_domain:get_object(ObjectReference, Domain) of
        {ok, Object} ->
            #'VersionedObject'{version = Version, object = Object};
        error ->
            {error, object_not_found}
    end.

-spec pull(dmt:version(), context()) -> dmt:history() | {error, version_not_found}.
pull(Version, Context) ->
    dmt_api_mg:get_history(Version, undefined, Context).

-spec commit(dmt:version(), dmt:commit(), context()) ->
    dmt:version() | {error, version_not_found | operation_conflict}.
commit(Version, Commit, Context) ->
    Snapshot = dmt_api_mg:commit(Version, Commit, Context),
    case Snapshot of
        #'Snapshot'{version = VersionNext} ->
            _ = dmt_cache:cache_snapshot(Snapshot),
            VersionNext;
        {error, _} ->
            Snapshot
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
            ip => IP,
            port => genlib_app:env(?MODULE, port, 8022),
            net_opts => #{},
            event_handler => woody_event_handler_default,
            handlers => [
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
        dmt_api_repository_handler
    }};
get_handler_spec(repository_client) ->
    {"/v1/domain/repository_client", {
        {dmsl_domain_config_thrift, 'RepositoryClient'},
        dmt_api_repository_client_handler
    }};
get_handler_spec(state_processor) ->
    {"/v1/stateproc", {
        {dmsl_state_processing_thrift, 'Processor'},
        dmt_api_state_processor
    }}.
