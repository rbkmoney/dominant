-module(dmt_client_tests_SUITE).
-include_lib("common_test/include/ct.hrl").

-export([all/0]).
-export([groups/0]).
-export([init_per_suite/1]).
-export([end_per_suite/1]).
-export([application_stop/1]).
-export([poll/1]).

-include_lib("dmt_proto/include/dmt_domain_config_thrift.hrl").

%%
%% tests descriptions
%%
-spec all() -> [term()].
all() ->
    [
        {group, basic_lifecycle}
    ].

-spec groups() -> [term()].
groups() ->
    [
        {basic_lifecycle, [sequence], [
            poll
        ]}
    ].

%%
%% starting/stopping
-spec init_per_suite(term()) -> term().
init_per_suite(C) ->
    ok = application:set_env(dmt, mgun_automaton_url, "http://machinegun:8022/v1/automaton_service"),
    {ok, Apps} = application:ensure_all_started(dmt),
    {ok, _PollerPid} = supervisor:start_child(dmt, #
        {id => dmt_poller, start => {dmt_poller, start_link, []}, restart => permanent}
    ),
    ok = dmt_mg:start(),
    [{apps, Apps}|C].

-spec end_per_suite(term()) -> term().
end_per_suite(C) ->
    [application_stop(App) || App <- proplists:get_value(apps, C)].

-spec application_stop(term()) -> term().
application_stop(App) ->
    application:stop(App).

%%
%% tests
-spec poll(term()) -> term().
poll(_C) ->
    Object = fixture_domain_object(1, <<"InsertFixture">>),
    Ref = fixture_object_ref(1),
    {'ObjectNotFound'} = (catch dmt_api_client:checkout_object({head, #'Head'{}}, Ref)),
    #'Snapshot'{version = Version1} = dmt_api_client:checkout({head, #'Head'{}}),
    Version2 = dmt_api_client:commit(Version1, #'Commit'{ops = [{insert, #'InsertOp'{object = Object}}]}),
    #'Snapshot'{version = Version1} = dmt:checkout({head, #'Head'{}}),
    object_not_found = (catch dmt:checkout_object({head, #'Head'{}}, Ref)),
    ok = dmt_poller:poll(),
    #'Snapshot'{version = Version2} = dmt:checkout({head, #'Head'{}}),
    #'VersionedObject'{object = Object} = dmt:checkout_object({head, #'Head'{}}, Ref).

fixture_domain_object(Ref, Data) ->
    {category, #'CategoryObject'{
        ref = #'CategoryRef'{id = Ref},
        data = #'Category'{name = Data, description = Data}
    }}.

fixture_object_ref(Ref) ->
    {category, #'CategoryRef'{id = Ref}}.
