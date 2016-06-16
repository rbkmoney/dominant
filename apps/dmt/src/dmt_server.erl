-module(dmt_server).
-behaviour(gen_server).

%%

-export([start_link/0]).

-export([checkout/2]).
-export([commit/1]).

%%

-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

-define(CACHE, ?MODULE).
-define(SERVER, ?MODULE).

%%

-spec start_link() -> {ok, pid()} | {error, term()}. % FIXME

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec checkout(dmt:ref(), dmt:key()) -> dmt:value().

checkout(Version, Key) ->
    case get_snapshot(Version) of
        {_Version, _Schema, #{Key := Value}} ->
            Value;
        {_Version, _Schema, #{}} ->
            throw(not_found)
    end.

-spec commit(dmt:commit()) -> ok.

commit(Commit) ->
    gen_server:call(?SERVER, {commit, Commit}).

get_snapshot(head) ->
    get_snapshot(ets:last(?CACHE));
get_snapshot(Version) ->
    case ets:lookup(?CACHE, Version) of
        [Snapshot] ->
            Snapshot;
        [] ->
            gen_server:call(?SERVER, {get_snapshot, Version})
    end.

%%

-record(state, {
    head :: dmt:snapshot()
}).

-type state() :: #state{}.

-spec init(_) -> {ok, state()}.

init(_) ->
    ok = init_cache(),
    Head = cache(dmt_storage:get_snapshot()),
    {ok, #state{head = Head}}.

-spec handle_call(term(), {pid(), term()}, state()) -> {reply, dmt:snapshot(), state()}.
handle_call({get_snapshot, Version}, _From, #state{head = Head} = State) ->
    %% TODO: use closest snapshot from the cache
    Snapshot = cache(rollback(Head, Version)),
    {reply, Snapshot, State};
handle_call({commit, Commit}, _From, #state{head = Head} = State) ->
    NewHead = cache(commit(Commit, Head)),
    {reply, ok, State#state{head = NewHead}};
handle_call(_Msg, _From, State) ->
    {noreply, State}.

-spec handle_cast(term(), state()) -> {noreply, state()}.
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

%% Internal

init_cache() ->
    ?CACHE = ets:new(?CACHE, [ordered_set, protected, named_table, {read_concurrency, true}]),
    ok.

-spec commit(dmt:commit(), dmt:snapshot()) -> dmt:snapshot().
commit({Schema, Operations}, {Version, Schema, Data}) ->
    {InvertedOps, NewData} = dmt_data:apply_operations(Operations, Data),
    NewSnapshot = {Version + 1, Schema, NewData},
    ok = dmt_storage:save({Schema, InvertedOps}, NewSnapshot),
    NewSnapshot.

cache(Snapshot) ->
    true = ets:insert(?CACHE, Snapshot),
    Snapshot.

rollback({FromVersion, _Schema, _Data} = Snapshot, ToVersion) when ToVersion =:= FromVersion ->
    Snapshot;
rollback({FromVersion, Schema, Data}, ToVersion) when ToVersion < FromVersion ->
    case dmt_storage:get_commit(FromVersion) of
        {Schema, Operations} ->
            {_InvertedOps, NewData} = dmt_data:apply_operations(Operations, Data),
            rollback({FromVersion - 1, Schema, NewData}, ToVersion);
        {_Schema, _Operations} ->
            throw(schema_not_supported)
    end.
