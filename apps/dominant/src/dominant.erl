%%% @doc Public API, supervisor and application startup.
%%% @end

-module(dominant).
-behaviour(supervisor).
-behaviour(application).

%% API
-export([start/0]).
-export([stop /0]).

%% Supervisor callbacks
-export([init/1]).

%% Application callbacks
-export([start/2]).
-export([stop /1]).

%%
%% API
%%
-spec start() ->
    {ok, _}.
start() ->
    application:ensure_all_started(dominant).

-spec stop() ->
    ok.
stop() ->
    application:stop(dominant).

%%
%% Supervisor callbacks
%%
init([]) ->
    {ok, {
        {one_for_all, 0, 1}, []
    }}.

%%
%% Application callbacks
%%
-spec start(normal, any()) ->
    {ok, pid()} | {error, any()}.
start(_StartType, _StartArgs) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec stop(any()) ->
    ok.
stop(_State) ->
    ok.
