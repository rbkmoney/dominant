-module(dmt_api).
-behaviour(application).
-behaviour(supervisor).

-export([get_handler_spec/1]).

-export([start/2]).
-export([stop/1]).
-export([init/1]).

%%

-spec start(application:start_type(), term()) -> {ok, pid()} | {error, term()}.

start(_StartType, _Args) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec stop(term()) -> ok.

stop(_State) ->
    ok.

%%

-spec init([]) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec(), ...]}}.

init([]) ->
    WoodyChildSpec = woody_server:child_spec(
        ?MODULE,
        #{
            ip => dmt_api_utils:get_hostname_ip(genlib_app:env(?MODULE, host, "dominant")),
            port => genlib_app:env(?MODULE, port, 8800),
            net_opts => [],
            event_handler => dmt_api_woody_event_handler,
            handlers => [
                get_handler_spec(repository),
                get_handler_spec(repository_client),
                get_handler_spec(mg_processor)
            ]
        }
    ),
    {ok, {#{strategy => one_for_one, intensity => 0, period => 1}, [WoodyChildSpec]}}.

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
