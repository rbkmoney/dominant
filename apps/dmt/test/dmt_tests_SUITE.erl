-module(dmt_tests_SUITE).
-include_lib("common_test/include/ct.hrl").

-export([all/0]).
-export([groups/0]).
-export([init_per_suite/1]).
-export([end_per_suite/1]).
-export([application_stop/1]).
-export([insert/1]).
-export([update/1]).
-export([delete/1]).

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
            insert,
            update,
            delete
        ]}
    ].

%%
%% starting/stopping
-spec init_per_suite(term()) -> term().
init_per_suite(C) ->
    ok = application:set_env(dmt, storage, dmt_storage_mgun),
    ok = application:set_env(dmt, mgun_automaton_url, "http://127.0.0.1:8022/v1/automaton_service"),
    {ok, Apps} = application:ensure_all_started(dmt_api),
    [{apps, Apps}|C].

-spec end_per_suite(term()) -> term().
end_per_suite(C) ->
    [application_stop(App) || App <- proplists:get_value(apps, C)].

-spec application_stop(term()) -> term().
application_stop(App) ->
    application:stop(App).

%%
%% tests
-spec insert(term()) -> term().
insert(_C) ->
    Object = fixture_domain_object(1, <<"InsertFixture">>),
    Ref = fixture_object_ref(1),
    object_not_found = (catch dmt:checkout_object({head, #'Head'{}}, Ref)),
    #'Snapshot'{version = Version1} = dmt:checkout({head, #'Head'{}}),
    Version2 = dmt:commit(Version1, #'Commit'{ops = [{insert, #'InsertOp'{object = Object}}]}),
    #'CheckoutObjectResult'{object = Object} = dmt:checkout_object({head, #'Head'{}}, Ref),
    object_not_found = (catch dmt:checkout_object({version, Version1}, Ref)),
    #'CheckoutObjectResult'{object = Object} = dmt:checkout_object({version, Version2}, Ref).

-spec update(term()) -> term().
update(_C) ->
    Object1 = fixture_domain_object(2, <<"UpdateFixture1">>),
    Object2 = fixture_domain_object(2, <<"UpdateFixture2">>),
    Ref = fixture_object_ref(2),
    #'Snapshot'{version = Version0} = dmt:checkout({head, #'Head'{}}),
    Version1 = dmt:commit(Version0, #'Commit'{ops = [{insert, #'InsertOp'{object = Object1}}]}),
    Version2 = dmt:commit(Version1, #'Commit'{ops = [{update, #'UpdateOp'{old_object = Object1, new_object = Object2}}]}),
    #'CheckoutObjectResult'{object = Object1} = dmt:checkout_object({version, Version1}, Ref),
    #'CheckoutObjectResult'{object = Object2} = dmt:checkout_object({version, Version2}, Ref).

-spec delete(term()) -> term().
delete(_C) ->
    Object = fixture_domain_object(3, <<"DeleteFixture">>),
    Ref = fixture_object_ref(3),
    #'Snapshot'{version = Version0} = dmt:checkout({head, #'Head'{}}),
    Version1 = dmt:commit(Version0, #'Commit'{ops = [{insert, #'InsertOp'{object = Object}}]}),
    Version2 = dmt:commit(Version1, #'Commit'{ops = [{remove, #'DeleteOp'{object = Object}}]}),
    #'CheckoutObjectResult'{object = Object} = dmt:checkout_object({version, Version1}, Ref),
    object_not_found = (catch dmt:checkout_object({version, Version2}, Ref)).

fixture_domain_object(Ref, Data) ->
    {party, #'PartyObject'{
        ref = #'PartyRef'{id = Ref},
        data = #'Party'{registered_name = Data}
    }}.

fixture_object_ref(Ref) ->
    {party, #'PartyRef'{id = Ref}}.
