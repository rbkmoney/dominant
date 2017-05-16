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
            get_snapshot(Version)
    end.

-spec get_closest(dmt_api_repository:version()) -> dmt_api_repository:snapshot().

get_closest(Version) ->
    gen_server:call(?SERVER, {get_closest, Version}).
%%

-record(state, {
    word_size,
    max_elements,
    max_memory
}).

-type state() :: #state{
    word_size :: pos_integer(),
    max_elements :: pos_integer(),
    max_memory :: pos_integer()
}.

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
    #{
        elements := MaxElements,
        memory := MaxMemory
    } = genlib_app:env(dmt_api, max_cache_size),
    {ok, #state{
        word_size = erlang:system_info(wordsize),
        max_elements = MaxElements,
        max_memory = MaxMemory
    }}.

-spec handle_call(term(), {pid(), term()}, state()) -> {reply, term(), state()}.

handle_call({get_closest, Version}, _From, State) ->
    {reply, closest_snapshot(Version), State};

handle_call(_Msg, _From, State) ->
    {noreply, State}.

-spec handle_cast(term(), state()) -> {noreply, state()}.

handle_cast({put, Snapshot}, State) ->
    true = ets:insert(?TABLE, Snapshot),
    ok = cleanup(State),
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
    Prev = ets:prev(?TABLE, Version),
    Next = ets:next(?TABLE, Version),
    {ok, Snapshot} = case {Prev, Next} of
        {'$end_of_table', '$end_of_table'} ->
            {ok, #'Snapshot'{version = 0, domain = dmt_domain:new()}};
        {'$end_of_table', Next} ->
            get_snapshot(Next);
        {Prev, '$end_of_table'} ->
            get_snapshot(Prev);
        _ ->
            case Next - Version < Version - Prev of
                true ->
                    get_snapshot(Next);
                false ->
                    get_snapshot(Prev)
            end
    end,
    Snapshot.

cleanup(#state{max_elements = MaxElements, max_memory = MaxMemory} = State) ->
    {Elements, Memory} = get_cache_size(State#state.word_size),
    case Elements > MaxElements orelse Memory > MaxMemory of
        true ->
            ok = remove_erleast(),
            cleanup(State);
        false ->
            ok
    end.

get_cache_size(Wordsize) ->
    Info = ets:info(?TABLE),
    {proplists:get_value(size, Info), Wordsize * proplists:get_value(memory, Info)}.

remove_erleast() ->
    % Naive implementation, but probably good enought
    remove_erleast(ets:first(?TABLE)).

remove_erleast('$end_of_table') ->
    ok;
remove_erleast(Key) ->
    true = ets:delete(?TABLE, Key),
    ok.
