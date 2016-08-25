-module(dmt_api).
-behaviour(application).
-behaviour(supervisor).

%% API
-export([checkout/1]).
-export([checkout_object/2]).
-export([pull/1]).
-export([commit/2]).
-export([validate_commit/3]).

%% behaviours
-export([start/2]).
-export([stop/1]).
-export([init/1]).

-include_lib("dmt/include/dmt_domain_config_thrift.hrl").

%% API

-spec checkout(dmt:ref()) -> dmt:snapshot().
checkout(Reference) ->
    try
        dmt_cache:checkout(Reference)
    catch
        {version_not_found, Version} ->
            dmt_cache:cache(Version, dmt_api_mg:get_history())
    end.

-spec checkout_object(dmt:ref(), dmt:object_ref()) ->
    dmt_domain_config_thrift:'VersionedObject'().
checkout_object(Reference, ObjectReference) ->
    #'Snapshot'{version = Version, domain = Domain} = checkout(Reference),
    Object = dmt_domain:get_object(ObjectReference, Domain),
    #'VersionedObject'{version = Version, object = Object}.

-spec pull(dmt:version()) -> dmt:history().
pull(Version) ->
    dmt_api_mg:get_history(Version).

-spec commit(dmt:version(), dmt:commit()) -> dmt:version().
commit(Version, Commit) ->
    ok = dmt_api_mg:commit(Version, Commit),
    Version + 1.

-spec validate_commit(dmt:version(), dmt:commit(), dmt:history()) -> ok.
validate_commit(Version, _Commit, History) ->
    %%TODO: actually validate commit
    LastVersion = case map_size(History) of
        0 ->
            0;
        _Size ->
            lists:max(maps:keys(History))
    end,
    case Version =:= LastVersion of
        true ->
            ok;
        false ->
            throw(bad_version)
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
                get_handler_spec(mg_processor)
            ]
        }
    ),
    Children = [API],
    {ok, {#{strategy => one_for_one, intensity => 10, period => 60}, Children}}.

-spec get_handler_spec(Which) -> {Path, {woody_t:service(), module(), term()}} when
    Which   :: repository | repository_client | mg_processor,
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
get_handler_spec(mg_processor) ->
    {"/v1/domain/mgun_processor", {
        {dmt_state_processing_thrift, 'Processor'},
        dmt_api_mgun_handler,
        []
    }}.
