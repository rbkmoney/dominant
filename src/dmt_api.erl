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

-include_lib("dmt/include/dmt_domain_config_thrift.hrl").

%% API

-type context() :: woody_client:context().

-spec checkout(dmt:ref(), context()) ->
    {dmt:snapshot() | {error, version_not_found}, context()}.
checkout(Reference, Context) ->
    try
        {dmt_cache:checkout(Reference), Context}
    catch
        version_not_found ->
            dmt_api_context:map(
                try_get_snapshot(Reference, Context),
                fun
                    (Snapshot = #'Snapshot'{}) -> dmt_cache:cache_snapshot(Snapshot);
                    (Error = {error, _})       -> Error
                end
            )
    end.

-spec try_get_snapshot(dmt:ref(), context()) ->
    {dmt:snapshot() | {error, version_not_found}, context()}.
try_get_snapshot(Reference, Context) ->
    {History, Context1} = dmt_api_mg:get_history(undefined, reference_to_limit(Reference), Context),
    {case {Reference, dmt_history:head(History)} of
        {{head, #'Head'{}}, Snapshot} ->
            Snapshot;
        {{version, V}, Snapshot = #'Snapshot'{version = V}} ->
            Snapshot;
        {{version, V1}, #'Snapshot'{version = V2}} when V1 > V2 ->
            {error, version_not_found}
    end, Context1}.

-spec reference_to_limit(dmt:ref()) -> pos_integer() | undefined.
reference_to_limit({head, #'Head'{}}) ->
    undefined;
reference_to_limit({version, Version}) ->
    Version.

-spec checkout_object(dmt:ref(), dmt:object_ref(), context()) ->
    {dmt_domain_config_thrift:'VersionedObject'(), context()}.
checkout_object(Reference, ObjectReference, Context) ->
    dmt_api_context:map(
        checkout(Reference, Context),
        fun
            (Snapshot = #'Snapshot'{}) -> try_get_object(ObjectReference, Snapshot);
            (Error = {error, _})       -> Error
        end
    ).

try_get_object(ObjectReference, #'Snapshot'{version = Version, domain = Domain}) ->
    case dmt_domain:get_object(ObjectReference, Domain) of
        {ok, Object} ->
            #'VersionedObject'{version = Version, object = Object};
        error ->
            {error, object_not_found}
    end.

-spec pull(dmt:version(), context()) ->
    {dmt:history() | {error, version_not_found}, context()}.
pull(Version, Context) ->
    dmt_api_mg:get_history(Version, undefined, Context).

-spec commit(dmt:version(), dmt:commit(), context()) ->
    {dmt:version() | {error, operation_conflict}, context()}.
commit(Version, Commit, Context) ->
    dmt_api_context:map(
        dmt_api_mg:commit(Version, Commit, Context),
        fun
            (Snapshot = #'Snapshot'{version = VersionNext}) ->
                _ = dmt_cache:cache_snapshot(Snapshot),
                VersionNext;
            (Error = {error, _}) ->
                Error
        end
    ).

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
        #'Snapshot'{version = Version} ->
            {error, {head_mismatch, Version}}
    end.

%% behaviours
-spec start(application:start_type(), term()) -> {ok, pid()} | {error, term()}.

start(_StartType, _Args) ->
    {ok, Pid} = supervisor:start_link({local, ?MODULE}, ?MODULE, []),
    {ok, _Context} = dmt_api_mg:start(dmt_api_context:new()),
    {ok, Pid}.

-spec stop(term()) -> ok.

stop(_State) ->
    ok.

%%

init([]) ->
    API = woody_server:child_spec(
        ?MODULE,
        #{
            ip => dmt_api_utils:get_hostname_ip(genlib_app:env(?MODULE, host, "dominant")),
            port => genlib_app:env(?MODULE, port, 8022),
            net_opts => [],
            event_handler => dmt_api_woody_event_handler,
            handlers => [
                get_handler_spec(repository),
                get_handler_spec(repository_client),
                get_handler_spec(state_processor)
            ]
        }
    ),
    Children = [API],
    {ok, {#{strategy => one_for_one, intensity => 10, period => 60}, Children}}.

-spec get_handler_spec(Which) -> {Path, {woody_t:service(), module(), term()}} when
    Which   :: repository | repository_client | state_processor,
    Path    :: iodata().

get_handler_spec(repository) ->
    {"/v1/domain/repository", {
        {dmt_domain_config_thrift, 'Repository'},
        dmt_api_repository_handler,
        []
    }};
get_handler_spec(repository_client) ->
    {"/v1/domain/repository_client", {
        {dmt_domain_config_thrift, 'RepositoryClient'},
        dmt_api_repository_client_handler,
        []
    }};
get_handler_spec(state_processor) ->
    {"/v1/stateproc", {
        {dmt_api_state_processing_thrift, 'Processor'},
        dmt_api_state_processor,
        []
    }}.
