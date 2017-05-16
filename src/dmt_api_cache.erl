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

-spec put(dmt_api_client:snapshot()) -> dmt_api_client:snapshot().

put(Snapshot) ->
    ok = gen_server:cast(?SERVER, {put, Snapshot}),
    Snapshot.

-spec get(dmt_api_client:version()) -> {ok, dmt_api_client:snapshot()} | {error, version_not_found}.

get(Version) ->
    get_snapshot(Version).

-spec get_latest() -> {ok, dmt_api_client:snapshot()} | {error, version_not_found}.

get_latest() ->
    case ets:last(?TABLE) of
        '$end_of_table' ->
            {error, version_not_found};
        Version ->
            get_snapshot(Version)
    end.

-spec get_closest(dmt_api_client:version()) -> dmt_api_client:snapshot().

get_closest(Version) ->
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

handle_call({get_closest, Version}, _From, State) ->
    {reply, closest_snapshot(Version), State};

handle_call(_Msg, _From, State) ->
    {noreply, State}.

-spec handle_cast(term(), state()) -> {noreply, state()}.

handle_cast({put, Snapshot}, State) ->
    true = ets:insert(?TABLE, Snapshot),
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

closest_snapshot(Version) ->
    case ets:last(?TABLE) of
        '$end_of_table' ->
            #'Snapshot'{version = 0, domain = dmt_domain:new()};
        LastVersion ->
            closest_snapshot(Version, LastVersion, ets:prev(?TABLE, LastVersion))
    end.

closest_snapshot(_Version, Closest, '$end_of_table') ->
    {ok, Snapshot} = get_snapshot(Closest),
    Snapshot;
closest_snapshot(Version, Closest, Key) ->
    case abs(Version - Key) < abs(Version - Closest) of
        true ->
            closest_snapshot(Version, Key, ets:prev(?TABLE, Key));
        false ->
            {ok, Snapshot} = get_snapshot(Closest),
            Snapshot
    end.


