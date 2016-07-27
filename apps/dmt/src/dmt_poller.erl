-module(dmt_poller).
-behaviour(gen_server).

-export([start_link/0]).
-export([poll/0]).

-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

-define(SERVER, ?MODULE).
-define(INTERVAL, 5000).

-include_lib("dmt_proto/include/dmt_domain_config_thrift.hrl").

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec poll() -> ok.
poll() ->
    gen_server:call(?SERVER, poll).

-record(state, {
    timer :: reference(),
    last_version = 0 :: dmt:version()
}).

-type state() :: #state{}.

-spec init(_) -> {ok, state()}.

init(_) ->
    Timer = erlang:send_after(?INTERVAL, self(), poll),
    {ok, #state{timer = Timer}}.

-spec handle_call(term(), {pid(), term()}, state()) -> {reply, term(), state()}.
handle_call(poll, _From, #state{last_version = LastVersion, timer = Timer} = State) ->
    _ = erlang:cancel_timer(Timer),
    NewLastVersion = pull(LastVersion),
    NewTimer = erlang:send_after(?INTERVAL, self(), poll),
    {reply, ok, State#state{timer = NewTimer, last_version = NewLastVersion}}.

-spec handle_cast(term(), state()) -> {noreply, state()}.
handle_cast(_Msg, State) ->
    {noreply, State}.

-spec handle_info(term(), state()) -> {noreply, state()}.
handle_info(timer, #state{last_version = LastVersion} = State) ->
    NewLastVersion = pull(LastVersion),
    Timer = erlang:send_after(?INTERVAL, self(), poll),
    {noreply, State#state{last_version = NewLastVersion, timer = Timer}};
handle_info(_Msg, State) ->
    {noreply, State}.

-spec terminate(term(), state()) -> ok.
terminate(_Reason, _State) ->
    ok.

-spec code_change(term(), state(), term()) -> {error, noimpl}.
code_change(_OldVsn, _State, _Extra) ->
    {error, noimpl}.

%% Internal
-spec pull(dmt:version()) -> dmt:version().
pull(LastVersion) ->
    FreshHistory = dmt_api_client:pull(LastVersion),
    OldHead = dmt_cache:checkout_head(),
    #'Snapshot'{version = NewLastVersion} = NewHead = dmt_history:head(FreshHistory, OldHead),
    ok = dmt_cache:cache_snapshot(NewHead),
    NewLastVersion.
