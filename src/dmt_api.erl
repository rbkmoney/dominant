-module(dmt_api).
-behaviour(application).
-behaviour(supervisor).

%% API

-export([start/2]).
-export([stop/1]).
-export([init/1]).

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
    HealthCheckers = genlib_app:env(?MODULE, health_checkers, []),
    API = woody_server:child_spec(
        ?MODULE,
        #{
            ip            => IP,
            port          => genlib_app:env(?MODULE, port, 8022),
            net_opts      => genlib_app:env(?MODULE, net_opts, []),
            event_handler => scoper_woody_event_handler,
            handlers      => get_repository_handlers(),
            additional_routes => [
                erl_health_handle:get_route(HealthCheckers)
            ]
        }
    ),
    Cache = dmt_api_cache:child_spec(),
    Children = [Cache, API],
    {ok, {#{strategy => one_for_one, intensity => 10, period => 60}, Children}}.

get_repository_handlers() ->
    Repository = genlib_app:env(?MODULE, repository, dmt_api_repository_v4),
    [
        get_handler_spec(repository, Repository),
        get_handler_spec(repository_client, Repository),
        get_handler_spec(state_processor, Repository)
    ].

-spec get_handler_spec(Which, Mod) -> {Path, {woody:service(), woody:handler(module())}} when
    Which   :: repository | repository_client | state_processor,
    Mod     :: module(),
    Path    :: iodata().

get_handler_spec(repository, Mod) ->
    {"/v1/domain/repository", {
        {dmsl_domain_config_thrift, 'Repository'},
        {dmt_api_repository_handler, Mod}
    }};
get_handler_spec(repository_client, Mod) ->
    {"/v1/domain/repository_client", {
        {dmsl_domain_config_thrift, 'RepositoryClient'},
        {dmt_api_repository_client_handler, Mod}
    }};
get_handler_spec(state_processor, Mod) ->
    {"/v1/stateproc", {
        {mg_proto_state_processing_thrift, 'Processor'},
        {dmt_api_automaton_handler, Mod}
    }}.
