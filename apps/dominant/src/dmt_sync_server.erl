-module(dmt_sync_server).
-behaviour(gen_server).

%%

-export([start_link/0]).

-export([head/0]).
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

%%

-spec start_link() -> {ok, pid()} | {error, term()}. % FIXME

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%

-spec head() ->
    {ok, dominant:revision()} | {error, notfound}.

head() ->
    deref(head).

-spec checkout(dominant:revision() | dominant:ref()) ->
    {ok, {dominant:revision(), dominant:data()}} | {error, notfound}.

checkout(Ref) when is_atom(Ref) ->
    dmt_utils:map_ok(deref(Ref), fun checkout/1);
checkout(Revision) when is_integer(Revision) ->
    dmt_utils:map_ok(read({data, Revision}), fun (Value) -> {Revision, Value} end).

-spec commit(dominant:revision(), dominant:operation()) ->
    {ok, dominant:revision()} | {error, term()}.

commit(Revision, Operation) ->
    gen_server:call(?SERVER, {commit, Revision, Operation}, infinity).

%%

read(Key) ->
    case read_cache(Key) of
        {ok, Value} ->
            {ok, Value};
        {error, notfound} ->
            gen_server:call(?SERVER, {read, Key}, infinity)
    end.

deref(Ref) when is_atom(Ref) ->
    read({ref, Ref}).

%%

-record(st, {
    cache :: ets:tid(),
    storage :: module(),
    head :: dominant:revision(),
    data :: dominant:data()
}).

-type state() :: #st{}.

-spec init(_) -> {ok, state()}.

init(_) ->
    {ok, construct_state()}.

-spec handle_call(_, {pid(), Tag :: reference()}, state()) ->
    {reply, _, state()} |
    {noreply, state()}.

handle_call({read, Key}, _From, State) ->
    {reply, read_storage(Key, State), State};

handle_call({commit, Revision, Operation}, _From, State) ->
    {Result, StateNext} = commit(Revision, Operation, State),
    {reply, Result, StateNext};

handle_call(Call, _From, State) ->
    lager:warning("unexpected call received: ~tp", [Call]),
    {noreply, State}.

-spec handle_cast(_, state()) ->
    {noreply, state()}.

handle_cast(Cast, State) ->
    lager:warning("unexpected cast received: ~tp", [Cast]),
    {noreply, State}.

-spec handle_info(_, state()) ->
    {noreply, state()}.

handle_info(Info, State) ->
    lager:warning("unexpected info received: ~tp", [Info]),
    {noreply, State}.

-spec terminate(Reason, state()) ->
    ok when
        Reason :: normal | shutdown | {shutdown, term()} | term().

terminate(_Reason, _State) ->
    ok.

-spec code_change(Vsn | {down, Vsn}, state(), term()) ->
    {error, noimpl} when
        Vsn :: term().

code_change(_OldVsn, _State, _Extra) ->
    {error, noimpl}.

%%

construct_state() ->
    populate_state(#st{
        cache = construct_cache(),
        storage = dmt_storage:init(dmt_storage_memory, #{})
    }).

construct_cache() ->
    ets:new(?CACHE, [protected, named_table, ordered_set, {read_concurrency, true}]).

populate_state(State) ->
    {ok, Revision} = read_storage_and_cache({ref, head}, State),
    {ok, Data} = read_storage_and_cache({data, Revision}, State),
    #st{head = Revision, data = Data}.

%%

commit(Revision, Operation, State = #st{head = Revision, data = Data}) ->
    DataNext = dmt_data:apply_operation(Operation, Data),
    StateNext = commit_data(DataNext, State),
    RevisionNext = StateNext#st.head,
    {{ok, RevisionNext}, StateNext};
commit(_Revision, _Operation, State) ->
    {{error, operation_conflict}, State}.

commit_data(Data, State = #st{head = WasRevision}) ->
    Revision = WasRevision + 1,
    {ok, Data} = write_storage_and_cache({data, Revision}, Data, State),
    {ok, Revision} = write_storage_and_cache({ref, head}, Revision, State),
    %% TODO not so dumb cache control?
    ok = erase_cache({data, WasRevision}),
    State#st{head = Revision, data = Data}.

%%

read_storage_and_cache(Key, State) ->
    dmt_utils:map_ok(read_storage(Key, State), fun (V) -> write_cache(Key, V) end).

read_storage(Key, #st{storage = Storage}) ->
    dmt_storage:read(Key, Storage).

write_storage_and_cache(Key, Value, State) ->
    dmt_utils:map_ok(write_storage(Key, Value, State), fun (V) -> write_cache(Key, V) end).

write_storage(Key, Value, #st{storage = Storage}) ->
    dmt_storage:write(Key, Value, Storage).

%%

read_cache(Key) ->
    case ets:lookup(?CACHE, Key) of
        [{Key, Value}] ->
            {ok, Value};
        [] ->
            {error, notfound}
    end.

write_cache(Key, Value) ->
    true = ets:insert(?CACHE, [{Key, Value}]),
    Value.

erase_cache(Key) ->
    true = ets:delete(?CACHE, Key),
    ok.
