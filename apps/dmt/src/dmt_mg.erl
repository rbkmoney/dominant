-module(dmt_mg).

-export([call/2]).
-export([start/0]).
-export([get_commit/1]).
-export([get_history/0]).
-export([get_history/1]).
-export([commit/2]).
-export([read_history/1]).

-include_lib("dmt_proto/include/dmt_state_processing_thrift.hrl").

-define(MG_TAG, <<"dmt_storage">>).

-spec call(atom(), list(term())) ->
     {ok, term()} | ok | no_return().
call(Method, Args) ->
    Request = {{dmt_state_processing_thrift, 'Automaton'}, Method, Args},
    Context = woody_client:new_context(
        woody_client:make_id(<<"dmt">>),
        dmt_api_event_handler
    ),
    {ok, MgunAutomatonUrl} = application:get_env(dmt, mgun_automaton_url),
    woody_client:call(Context, Request, #{url => MgunAutomatonUrl}).

-spec start() -> ok.
start() ->
    {{ok, {_, _}}, _} = call(start, {'Args', <<>>}),
    ok.

-spec get_commit(dmt:version()) -> dmt:commit().
get_commit(Id) ->
    #{Id := Commit} = get_history(),
    Commit.

%% TODO: add range requests after they are fixed in mg
-spec get_history() -> dmt:history().
get_history() ->
    get_history(undefined).

-spec get_history(dmt:version() | undefined) -> dmt:history().
get_history(After) ->
    {{ok, History}, _Context} = call(getHistory, [{tag, ?MG_TAG}, #'HistoryRange'{'after' = After}]),
    read_history(History).

-spec commit(dmt:version(), dmt:commit()) -> ok.
commit(Version, Commit) ->
    Call = <<"commit", (term_to_binary({Version, Commit}))/binary>>,
    {{ok, <<"ok">>}, _Context} = call(call, [{tag, ?MG_TAG}, Call]),
    ok.

%% utils

-spec read_history([dmt_state_processing_thrift:'Event'()]) -> dmt:history().
read_history(Events) ->
    read_history(Events, #{}).

-spec read_history([dmt_state_processing_thrift:'Event'()], dmt:history()) ->
    dmt:history().
read_history([], History) ->
    History;
read_history([#'Event'{id = Id, event_payload = BinaryPayload} | Rest], History) ->
    read_history(Rest, History#{Id => binary_to_term(BinaryPayload)}).
