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

-include_lib("dmsl/include/dmsl_domain_config_thrift.hrl").

%%

-spec start_link() -> {ok, pid()} | {error, term()}. % FIXME

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
    case ets:last(?TABLE) of
        '$end_of_table' ->
            {error, version_not_found};
        Version ->
            case get_snapshot(Version) of
                {ok, _} = Result ->
                    Result;
                {error, version_not_found} ->
                    gen_server:call(?SERVER, get_latest)
            end
    end.

-spec get_closest(dmt_api_repository:version()) -> dmt_api_repository:snapshot().

get_closest(Version) ->
    Prev = ets:prev(?TABLE, Version),
    Next = ets:next(?TABLE, Version),
    get_closest(Prev, Next, Version).

get_closest('$end_of_table', '$end_of_table', _Version) ->
    #'Snapshot'{version = 0, domain = dmt_domain:new()};

get_closest('$end_of_table', Next, Version) ->
    check_closest_snapshot(get_snapshot(Next), Version);

get_closest(Prev, '$end_of_table', Version) ->
    check_closest_snapshot(get_snapshot(Prev), Version);

get_closest(Prev, Next, Version) ->
    case Next - Version < Version - Prev of
        true ->
            check_closest_snapshot(get_snapshot(Next), Version);
        false ->
            check_closest_snapshot(get_snapshot(Prev), Version)
    end.

check_closest_snapshot({ok, Snapshot}, _Version) ->
    Snapshot;

check_closest_snapshot({error, version_not_found}, Version) ->
    gen_server:call(?SERVER, {get_closest, Version}).

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

handle_call(get_latest, _From, State) ->
    {reply, get_latest(), State};

handle_call({get_closest, Version}, _From, State) ->
    {reply, get_closest(Version), State};

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

get_snapshot(Version) ->
    case ets:lookup(?TABLE, Version) of
        [Snapshot] ->
            {ok, Snapshot};
        [] ->
            {error, version_not_found}
    end.

cleanup() ->
    {Elements, Memory} = get_cache_size(),
    CacheLimits = genlib_app:env(dmt_api, max_cache_size),
    MaxElements = genlib_map:get(elements, CacheLimits, 100),
    MaxMemory = genlib_map:get(memory, CacheLimits, 100*1024*1024), % 100Mb by default
    case Elements > MaxElements orelse Memory > MaxMemory of
        true ->
            ok = remove_erliest(),
            cleanup();
        false ->
            ok
    end.

get_cache_size() ->
    WordSize = erlang:system_info(wordsize),
    Info = ets:info(?TABLE),
    {proplists:get_value(size, Info), WordSize * proplists:get_value(memory, Info)}.

remove_erliest() ->
    % Naive implementation, but probably good enought
    remove_erliest(ets:first(?TABLE)).

remove_erliest('$end_of_table') ->
    ok;
remove_erliest(Key) ->
    true = ets:delete(?TABLE, Key),
    ok.
