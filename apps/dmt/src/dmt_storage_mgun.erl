-module(dmt_storage_mgun).

-export([call_mg/2]).
-export([start/0]).
-export([get_commit/1]).
-export([get_history/0]).
-export([commit/1]).

-include_lib("dmt_proto/include/dmt_state_processing_thrift.hrl").

-define(MG_TAG, <<"dmt_storage">>).

-spec call_mg(atom(), list(term())) ->
     {ok, term()} | ok | no_return().
call_mg(Method, Args) ->
    Request = {{dmt_state_processing_thrift, 'Automaton'}, Method, Args},
    Context = woody_client:new_context(
        woody_client:make_id(<<"dmt">>),
        dmt_api_event_handler
    ),
    {ok, MgunAutomatonUrl} = application:get_env(dmt, mgun_automaton_url),
    woody_client:call(Context, Request, #{url => MgunAutomatonUrl}).

-spec start() -> binary().
start() ->
    {{ok, {_, M}}, _} = call_mg(start, {'Args', <<>>}),
    M.

-spec get_commit(dmt:version()) -> dmt:commit().
get_commit(Id) ->
    #{Id := Commit} = get_history(),
    Commit.

-spec get_history() -> dmt:history().
get_history() ->
    {{ok, Events}, _Context} = dmt_storage_mgun:call_mg(getHistory, [{tag, ?MG_TAG}, #'HistoryRange'{}]),
    maps:from_list([{Id, binary_to_term(Body)} || #'Event'{id = Id, event_payload = Body} <- Events]).

-spec commit(dmt:commit()) -> ok.
commit(Commit) ->
    {{ok, <<"ok">>}, _Context} = call_mg(call, [{tag, ?MG_TAG}, term_to_binary(Commit)]),
    ok.
