-module(dmt_api_tests_SUITE).
-include_lib("common_test/include/ct.hrl").

-export([all/0]).
-export([groups/0]).
-export([init_per_suite/1]).
-export([end_per_suite/1]).

-export([insert/1]).
-export([update/1]).
-export([delete/1]).

-include_lib("dmsl/include/dmsl_domain_config_thrift.hrl").

%% tests descriptions

-type config() :: [{atom(), term()}].

-define(config(Key, C), (element(2, lists:keyfind(Key, 1, C)))).

-type test_case_name() :: atom().
-type group_name() :: atom().

-spec all() -> [{group, group_name()}].
all() ->
    [
        {group, basic_lifecycle}
    ].

-spec groups() -> [{group_name(), list(), [test_case_name()]}].
groups() ->
    [
        {basic_lifecycle, [sequence, {repeat, 10}, shuffle], [
            insert,
            update,
            delete
        ]}
    ].

%%
%% starting/stopping
-spec init_per_suite(config()) -> config().
init_per_suite(C) ->
    Apps =
        genlib_app:start_application_with(lager, [
            {async_threshold, 1},
            {async_threshold_window, 0},
            {error_logger_hwm, 600},
            {suppress_application_start_stop, true},
            {handlers, [
                {lager_common_test_backend, [debug, false]}
            ]}
        ]) ++
        genlib_app:start_application_with(dmt_api, [
            {automaton_service_url, "http://machinegun:8022/v1/automaton"}
        ]),
    [{suite_apps, Apps}, {counter, 1} | C].

-spec end_per_suite(config()) -> term().
end_per_suite(C) ->
    [application:stop(App) || App <- lists:reverse(?config(suite_apps, C))].

%%
%% tests

-spec insert(term()) -> term().
insert(_C) ->
    ID = next_id(),
    Object = fixture_domain_object(ID, <<"InsertFixture">>),
    Ref = fixture_object_ref(ID),
    #'ObjectNotFound'{} = (catch dmt_client_api:checkout_object({head, #'Head'{}}, Ref)),
    #'Snapshot'{version = Version1} = dmt_client_api:checkout({head, #'Head'{}}),
    Version2 = dmt_client_api:commit(Version1, #'Commit'{ops = [{insert, #'InsertOp'{object = Object}}]}),
    #'VersionedObject'{object = Object} = dmt_client_api:checkout_object({head, #'Head'{}}, Ref),
    #'ObjectNotFound'{} = (catch dmt_client_api:checkout_object({version, Version1}, Ref)),
    #'VersionedObject'{object = Object} = dmt_client_api:checkout_object({version, Version2}, Ref).

-spec update(term()) -> term().
update(_C) ->
    ID = next_id(),
    Object1 = fixture_domain_object(ID, <<"UpdateFixture1">>),
    Object2 = fixture_domain_object(ID, <<"UpdateFixture2">>),
    Ref = fixture_object_ref(ID),
    #'Snapshot'{version = Version0} = dmt_client_api:checkout({head, #'Head'{}}),
    Version1 = dmt_client_api:commit(Version0, #'Commit'{ops = [{insert, #'InsertOp'{object = Object1}}]}),
    Version2 = dmt_client_api:commit(
        Version1,
        #'Commit'{ops = [{update, #'UpdateOp'{old_object = Object1, new_object = Object2}}]}
    ),
    #'VersionedObject'{object = Object1} = dmt_client_api:checkout_object({version, Version1}, Ref),
    #'VersionedObject'{object = Object2} = dmt_client_api:checkout_object({version, Version2}, Ref).

-spec delete(term()) -> term().
delete(_C) ->
    ID = next_id(),
    Object = fixture_domain_object(ID, <<"DeleteFixture">>),
    Ref = fixture_object_ref(ID),
    #'Snapshot'{version = Version0} = dmt_client_api:checkout({head, #'Head'{}}),
    Version1 = dmt_client_api:commit(Version0, #'Commit'{ops = [{insert, #'InsertOp'{object = Object}}]}),
    Version2 = dmt_client_api:commit(Version1, #'Commit'{ops = [{remove, #'RemoveOp'{object = Object}}]}),
    #'VersionedObject'{object = Object} = dmt_client_api:checkout_object({version, Version1}, Ref),
    #'ObjectNotFound'{} = (catch dmt_client_api:checkout_object({version, Version2}, Ref)).

next_id() ->
    erlang:system_time(micro_seconds) band 16#7FFFFFFF.

fixture_domain_object(Ref, Data) ->
    {category, #domain_CategoryObject{
        ref = #domain_CategoryRef{id = Ref},
        data = #domain_Category{name = Data, description = Data}
    }}.

fixture_object_ref(Ref) ->
    {category, #domain_CategoryRef{id = Ref}}.
