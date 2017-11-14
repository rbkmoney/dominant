-module(dmt_api_repository_v2).
-behaviour(dmt_api_repository).

%%
%% This is old, depricated version of domain repository.
%%

-include_lib("dmsl/include/dmsl_domain_config_thrift.hrl").
-include_lib("mg_proto/include/mg_proto_state_processing_thrift.hrl").

-define(NS  , <<"domain-config">>).
-define(ID  , <<"primary/v2">>).


%% API

-export([checkout/2]).
-export([pull/2]).
-export([commit/3]).

-export([get_history/2]).
-export([get_history/3]).

-type context()         :: woody_context:ctx().
-type history_range()   :: mg_proto_state_processing_thrift:'HistoryRange'().
-type machine()         :: mg_proto_state_processing_thrift:'Machine'().
-type history()         :: mg_proto_state_processing_thrift:'History'().

-type ref()             :: dmsl_domain_config_thrift:'Reference'().
-type snapshot()        :: dmt_api_repository:snapshot().
-type commit()          :: dmt_api_repository:commit().

%%

-spec checkout(ref(), context()) ->
    {ok, snapshot()} |
    {error, version_not_found}.

checkout({head, #'Head'{}} = Head, Context) ->
    % we should reply with latest version
    dmt_api_repository_v3:checkout(Head, Context);
checkout({version, Version}, Context) ->
    case dmt_api_cache:get(Version) of
        {ok, Snapshot} ->
            {ok, Snapshot};
        {error, version_not_found} ->
            case try_get_snapshot(Version, Context) of
                {ok, Snapshot} ->
                    {ok, dmt_api_cache:put(Snapshot)};
                {error, version_not_found} ->
                    {error, version_not_found}
            end
    end.

-spec pull(dmt_api_repository:version(), context()) ->
    {ok, dmt_api_repository:history()} |
    {error, version_not_found}.

pull(Version, Context) ->
    After = get_event_id(Version),
    case get_history_by_range(#mg_stateproc_HistoryRange{'after' = After}, Context) of
        History when is_map(History) ->
            {ok, History};
        Error ->
            Error
    end.


-spec commit(dmt_api_repository:version(), commit(), context()) -> no_return().

commit(_, _, _) ->
    error('Commits to repository v2 are prohibited').

%%

-spec get_history(pos_integer() | undefined, context()) ->
    dmt_api_repository:history().
get_history(Limit, Context) ->
    get_history_by_range(#'mg_stateproc_HistoryRange'{'after' = undefined, 'limit' = Limit}, Context).

-spec get_history(dmt_api_repository:version(), pos_integer() | undefined, context()) ->
    {ok, dmt_api_repository:history()} | {error, version_not_found}.
get_history(Version, Limit, Context) ->
    After = get_event_id(Version),
    case get_history_by_range(#'mg_stateproc_HistoryRange'{'after' = After, 'limit' = Limit}, Context) of
        History when is_map(History) ->
            {ok, History};
        Error ->
            Error
    end.

%%

get_event_id(ID) when is_integer(ID) andalso ID > 0 ->
    ID;
get_event_id(0) ->
    undefined.

-spec get_history_by_range(history_range(), context()) ->
    dmt_api_repository:history() | {error, version_not_found}.
get_history_by_range(HistoryRange, Context) ->
    case dmt_api_automaton_client:get_history(?NS, ?ID, HistoryRange, Context) of
        {ok, History} ->
            read_history(History);
        {error, #'mg_stateproc_MachineNotFound'{}} ->
            #{};
        {error, #'mg_stateproc_EventNotFound'{}} ->
            {error, version_not_found}
    end.

-spec try_get_snapshot(dmt_api_repository:version(), context()) ->
    {ok, snapshot()} | {error, version_not_found}.

try_get_snapshot(Version, Context) ->
    ClosestSnapshot = ensure_snapshot(dmt_api_cache:get_closest(Version)),
    From = min(Version, ClosestSnapshot#'Snapshot'.version),
    Limit = abs(Version - ClosestSnapshot#'Snapshot'.version),
    case get_history(From, Limit, Context) of
        {ok, History} when map_size(History) =:= Limit ->
            %% TO DO: Need to fix dmt_history:travel. It can return {error, ...}
            {ok, Snapshot} = dmt_history:travel(Version, History, ClosestSnapshot),
            {ok, Snapshot};
        {ok, #{}} ->
            {error, version_not_found};
        {error, version_not_found} ->
            {error, version_not_found}
    end.

ensure_snapshot({ok, Snapshot}) ->
    Snapshot;
ensure_snapshot({error, version_not_found}) ->
    #'Snapshot'{version = 0, domain = dmt_domain:new()}.

-spec read_history(machine() | history()) ->
    dmt_api_repository:history().
read_history(#'mg_stateproc_Machine'{history = Events}) ->
    read_history(Events);
read_history(Events) ->
    read_history(Events, #{}).

-spec read_history([mg_proto_state_processing_thrift:'Event'()], dmt_api_repository:history()) ->
    dmt_api_repository:history().
read_history([], History) ->
    History;
read_history([#'mg_stateproc_Event'{id = Id, event_payload = EventData} | Rest], History) ->
    {commit, Commit} = decode_event(EventData),
    read_history(Rest, History#{Id => Commit}).

decode_event({arr, [{str, <<"commit">>}, Commit]}) ->
    {commit, decode(commit, Commit)}.

decode(T, V) ->
    dmt_api_thrift_utils:decode(msgpack, get_type_info(T), V).

get_type_info(commit) ->
    {struct, struct, {dmsl_domain_config_thrift, 'Commit'}}.
