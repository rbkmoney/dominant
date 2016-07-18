-module(dmt_cache).
-behaviour(gen_server).

%%

-export([start_link/0]).

-export([checkout/1]).
-export([cache/1]).
-export([commit/1]).

%%

-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

-define(TABLE, ?MODULE).
-define(SERVER, ?MODULE).

-include_lib("dmt_proto/include/dmt_domain_config_thrift.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

%%

-spec start_link() -> {ok, pid()} | {error, term()}. % FIXME

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec checkout(dmt:ref()) -> dmt:snapshot().
checkout({head, #'Head'{}}) ->
    checkout({version, head()});
checkout({version, Version}) ->
    case ets:lookup(?TABLE, Version) of
        [Snapshot] ->
            Snapshot;
        [] ->
            gen_server:call(?SERVER, {cache, Version})
    end.

-spec cache(dmt:version()) -> dmt:snapshot().
cache(Version) ->
    gen_server:call(?SERVER, {cache, Version}).

-spec commit(dmt:commit()) -> ok.
commit(Commit) ->
    gen_server:call(?SERVER, {commit, Commit}).

%%

-record(state, {
}).

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
handle_call({cache, Version}, _From, State) ->
    Closest = closest_snapshot(Version),
    Snapshot = dmt_history:travel(Version, dmt_mg:get_history(), Closest),
    true = ets:insert(?TABLE, Snapshot),
    {reply, Snapshot, State};
handle_call({commit, #'Commit'{ops = Ops}}, _From, State) ->
    #'Snapshot'{version = Version, domain = Domain} = checkout({head, #'Head'{}}),
    NewSnapshot = #'Snapshot'{
        version = Version + 1,
        domain = dmt_domain:apply_operations(Ops, Domain)
    },
    true = ets:insert(?TABLE, NewSnapshot),
    {reply, NewSnapshot, State};
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

%% internal

-spec head() -> dmt:version().
head() ->
    case ets:last(?TABLE) of
        '$end_of_table' ->
            #'Snapshot'{version = Version} = dmt_history:head(dmt_mg:get_history()),
            Version;
        Version ->
            Version
    end.

-spec closest_snapshot(dmt:version()) -> dmt:snapshot().
closest_snapshot(Version) ->
    CachedVersions = ets:select(?TABLE, ets:fun2ms(fun (#'Snapshot'{version = V}) -> V end)),
    Closest = lists:foldl(fun (V, Acc) ->
        case abs(V - Version) =< abs(Acc - Version) of
            true ->
                V;
            false ->
                Acc
        end
    end, 0, CachedVersions),
    case Closest of
        0 ->
            #'Snapshot'{version = 0, domain = dmt_domain:new()};
        Closest ->
            ets:lookup(?TABLE, Closest)
    end.
