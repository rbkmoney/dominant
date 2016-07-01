-module(dmt_server).
-behaviour(gen_server).

%%

-export([start_link/0]).

-export([checkout/1]).
-export([commit/2]).

%%

-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

-define(CACHE, ?MODULE).
-define(SERVER, ?MODULE).

-include_lib("dmt_proto/include/dmt_domain_config_thrift.hrl").

%%

-spec start_link() -> {ok, pid()} | {error, term()}. % FIXME

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).


-spec commit(dmt:version(), dmt:commit()) -> dmt:version().
commit(Version, Commit) ->
    gen_server:call(?SERVER, {commit, Version, Commit}).

-spec checkout(dmt:ref()) -> dmt:snapshot().
checkout({head, #'Head'{}}) ->
    checkout({version, head_cache()});
checkout({version, Version}) ->
    case checkout_cache(Version) of
        [Snapshot] ->
            Snapshot;
        [] ->
            gen_server:call(?SERVER, {checkout, Version})
    end.

%%

-record(state, {
    head :: dmt:snapshot()
}).

-type state() :: #state{}.

-spec init(_) -> {ok, state()}.

init(_) ->
    ok = init_cache(),
    {ok, #state{}}.

-spec handle_call(term(), {pid(), term()}, state()) -> {reply, term(), state()}.
handle_call(Call, From, #state{head = undefined} = State) ->
    History = dmt_storage:get_history(),
    Head = cache(rollforward(new_snapshot(), History)),
    handle_call(Call, From, State#state{head = Head});
handle_call({checkout, Version}, _From, #state{head = Head} = State) ->
    %% TODO: use closest snapshot from the cache
    Snapshot = cache(rollback(Head, Version)),
    {reply, Snapshot, State};
handle_call({commit, Version, Commit}, _From, #state{head = Head} = State) ->
    #'Snapshot'{version = NewVersion} = NewHead = cache(commit(Version, Commit, Head)),
    {reply, NewVersion, State#state{head = NewHead}};
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
    EtsOpts = [
        named_table,
        ordered_set,
        protected,
        {read_concurrency, true},
        {keypos, #'Snapshot'.version}
    ],
    ?CACHE = ets:new(?CACHE, EtsOpts),
    ok.

head_cache() ->
    case ets:last(?CACHE) of
        '$end_of_table' ->
            0;
        Version ->
            Version
    end.

checkout_cache(Version) ->
    ets:lookup(?CACHE, Version).

-spec commit(dmt:version(), dmt:commit(), dmt:snapshot()) -> dmt:snapshot().
commit(Version, #'Commit'{ops = Ops} = Commit, #'Snapshot'{version = Version, domain = Domain}) ->
    NewDomain = dmt_domain:apply_operations(Ops, Domain),
    NewSnapshot = #'Snapshot'{version = Version + 1, domain = NewDomain},
    ok = dmt_storage:commit(Commit),
    NewSnapshot;
commit(_Version, _Commit, _Snapshot) ->
    throw(version_out_of_date).

cache(Snapshot) ->
    true = ets:insert(?CACHE, Snapshot),
    Snapshot.

-spec rollback(dmt:snapshot(), dmt:version()) -> dmt:snapshot().
rollback(#'Snapshot'{version = FromVersion} = Snapshot, ToVersion) when ToVersion =:= FromVersion ->
    Snapshot;
rollback(#'Snapshot'{version = FromVersion, domain = Domain}, ToVersion) when ToVersion < FromVersion ->
    #'Commit'{ops = Ops} = dmt_storage:get_commit(FromVersion),
    NewDomain = dmt_domain:revert_operations(Ops, Domain),
    rollback(#'Snapshot'{version = FromVersion - 1, domain = NewDomain}, ToVersion).

-spec rollforward(dmt:snapshot(), dmt:history()) -> dmt:snapshot().
rollforward(#'Snapshot'{version = Version, domain = Domain} = Snapshot, History) ->
    case maps:find(Version + 1, History) of
        {ok, #'Commit'{ops = Ops}} ->
            NewDomain = dmt_domain:apply_operations(Ops, Domain),
            rollforward(#'Snapshot'{version = Version + 1, domain = NewDomain}, History);
        error ->
            Snapshot
    end.

new_snapshot() ->
    #'Snapshot'{version = 0, domain = #{}}.
