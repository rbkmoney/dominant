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
    {ok, {
        #{strategy => one_for_one, intensity => 0, period => 1},
        [woody_server:child_spec(
            ?MODULE,
            #{
                ip => dmt_api_utils:get_hostname_ip(genlib_app:env(?MODULE, host, "localhost")),
                port => genlib_app:env(?MODULE, port, 8800),
                net_opts => [],
                event_handler => dmt_api_configurator_handler,
                handlers => [get_handler_spec(configurator)]
            }
        )]
    }}.

-spec get_handler_spec(Which) -> {Path, {woody_t:service(), module(), term()}} when
    Which   :: configurator,
    Path    :: iodata().

get_handler_spec(configurator) ->
    {"/v1/domain/configurator", {
        {dmt_domain_config, 'Configurator'},
        dmt_api_configurator_handler,
        []
    }}.
