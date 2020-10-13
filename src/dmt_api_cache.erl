-module(dmt_api_cache).

-behaviour(gen_server).

%%

-export([start_link/0]).
-export([child_spec/0]).

-export([put/1]).
-export([get/1]).
-export([get_latest/0]).
-export([get_closest/1]).

%%

-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

-define(TABLE, ?MODULE).
-define(SERVER, ?MODULE).

-include_lib("damsel/include/dmsl_domain_config_thrift.hrl").

%%

% FIXME
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec child_spec() -> supervisor:child_spec().
child_spec() ->
    #{id => ?MODULE, start => {?MODULE, start_link, []}, restart => permanent}.

-spec put(dmt_api_repository:snapshot()) -> dmt_api_repository:snapshot().
put(Snapshot) ->
    ok = gen_server:cast(?SERVER, {put, Snapshot}),
    Snapshot.

-spec get(dmt_api_repository:version()) -> {ok, dmt_api_repository:snapshot()} | {error, version_not_found}.
get(Version) ->
    get_snapshot(Version).

-spec get_latest() -> {ok, dmt_api_repository:snapshot()} | {error, version_not_found}.
get_latest() ->
    exec_with_synchronous_retry(fun() -> get_latest_snapshot() end).

-spec get_closest(dmt_api_repository:version()) -> {ok, dmt_api_repository:snapshot()} | {error, version_not_found}.
get_closest(Version) ->
    exec_with_synchronous_retry(fun() -> get_closest_snapshot(Version) end).

%%

-record(state, {}).

-type state() :: #state{}.

-spec init(_) -> {ok, state()}.
init(_) ->
    EtsOpts = [
        named_table,
        ordered_set,
        protected,
        {read_concurrency, true},
        {keypos, #'Snapshot'.version}
    ],
    ?TABLE = ets:new(?TABLE, EtsOpts),
    {ok, #state{}}.

-spec handle_call(term(), {pid(), term()}, state()) -> {reply, term(), state()}.
handle_call({exec, F}, _From, State) ->
    {reply, F(), State};
handle_call(_Msg, _From, State) ->
    {noreply, State}.

-spec handle_cast(term(), state()) -> {noreply, state()}.
handle_cast({put, Snapshot}, State) ->
    true = ets:insert(?TABLE, Snapshot),
    ok = cleanup(),
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

-spec handle_info(term(), state()) -> {noreply, state()}.
handle_info(_Msg, State) ->
    {noreply, State}.

-spec terminate(term(), state()) -> ok.
terminate(_Reason, _State) ->
    ok.

-spec code_change(term(), state(), term()) -> {error, noimpl}.
code_change(_OldVsn, _State, _Extra) ->
    {error, noimpl}.

%% internal

exec_with_synchronous_retry(F) ->
    case F() of
        {ok, Result} ->
            {ok, Result};
        {error, _} ->
            gen_server:call(?SERVER, {exec, F})
    end.

get_snapshot(Version) ->
    case ets:lookup(?TABLE, Version) of
        [Snapshot] ->
            {ok, Snapshot};
        [] ->
            {error, version_not_found}
    end.

get_latest_snapshot() ->
    case ets:last(?TABLE) of
        '$end_of_table' ->
            {error, version_not_found};
        Version ->
            get_snapshot(Version)
    end.

get_closest_snapshot(Version) ->
    Prev = ets:prev(?TABLE, Version),
    Next = ets:next(?TABLE, Version),
    get_closest_snapshot(Prev, Next, Version).

get_closest_snapshot('$end_of_table', '$end_of_table', _Version) ->
    {error, version_not_found};
get_closest_snapshot('$end_of_table', Next, _Version) ->
    get_snapshot(Next);
get_closest_snapshot(Prev, '$end_of_table', _Version) ->
    get_snapshot(Prev);
get_closest_snapshot(Prev, Next, Version) ->
    case Next - Version < Version - Prev of
        true ->
            get_snapshot(Next);
        false ->
            get_snapshot(Prev)
    end.

cleanup() ->
    CacheSize = get_cache_size(),
    % 50Mb by default
    CacheLimit = genlib_app:env(dmt_api, max_cache_size, 52428800),
    case CacheSize > CacheLimit of
        true ->
            ok = remove_earliest(),
            cleanup();
        false ->
            ok
    end.

get_cache_size() ->
    erlang:system_info(wordsize) * ets:info(?TABLE, memory).

remove_earliest() ->
    % Naive implementation, but probably good enough
    remove_earliest(ets:first(?TABLE)).

remove_earliest('$end_of_table') ->
    ok;
remove_earliest(Key) ->
    true = ets:delete(?TABLE, Key),
    ok.
