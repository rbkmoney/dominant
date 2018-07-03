-module(dmt_api_tests_SUITE).
-include_lib("common_test/include/ct.hrl").

-export([all/0]).
-export([groups/0]).
-export([init_per_suite/1]).
-export([end_per_suite/1]).
-export([init_per_group/2]).
-export([end_per_group/2]).

-export([pull_commit/1]).
-export([insert/1]).
-export([update/1]).
-export([delete/1]).
-export([migration_success/1]).

-include_lib("dmsl/include/dmsl_domain_config_thrift.hrl").

%% tests descriptions

-type config() :: [{atom(), term()}].

-define(config(Key, C), (element(2, lists:keyfind(Key, 1, C)))).

-type test_case_name() :: atom().
-type group_name() :: atom().

-spec all() -> [{group, group_name()}].
all() ->
    [
        {group, basic_lifecycle_v3},
        {group, migration_to_v4},
        {group, basic_lifecycle_v4}
    ].

-spec groups() -> [{group_name(), list(), [test_case_name()]}].
groups() ->
    [
        {basic_lifecycle_v3, [sequence], [
            pull_commit,
            {group, basic_lifecycle}
        ]},
        {basic_lifecycle_v4, [sequence], [
            pull_commit,
            {group, basic_lifecycle}
        ]},
        {basic_lifecycle, [sequence, {repeat, 10}, shuffle], [
            insert,
            update,
            delete
        ]},
        {migration_to_v4, [sequence], [
            migration_success
        ]}
    ].

%%
%% starting/stopping
-spec init_per_suite(config()) -> config().
init_per_suite(C) ->
    Apps =
        genlib_app:start_application(sasl) ++ %% Added this to clean up logs
        genlib_app:start_application_with(lager, [
            {async_threshold, 1},
            {async_threshold_window, 0},
            {error_logger_hwm, 600},
            {suppress_application_start_stop, true},
            {handlers, [
                % {lager_common_test_backend, [warning, {lager_logstash_formatter, []}]}
                {lager_common_test_backend, warning}
            ]}
        ]) ++
        genlib_app:start_application_with(scoper, [
            {storage, scoper_storage_lager}
        ]) ++
        genlib_app:start_application_with(dmt_client, [
            {cache_update_interval, 5000}, % milliseconds
            {max_cache_size, #{
                elements => 20,
                memory => 52428800 % 50Mb
            }},
            {service_urls, #{
                'Repository' => <<"dominant:8022/v1/domain/repository">>,
                'RepositoryClient' => <<"dominant:8022/v1/domain/repository_client">>
            }}
        ]),
    [{suite_apps, Apps} | C].

-spec end_per_suite(config()) -> term().
end_per_suite(C) ->
    genlib_app:stop_unload_applications(?config(suite_apps, C)).

-spec init_per_group(group_name(), config()) -> config().
init_per_group(basic_lifecycle_v3, C) ->
    [{group_apps, start_with_repository(dmt_api_repository_v3)} | C];
init_per_group(basic_lifecycle_v4, C) ->
    [{group_apps, start_with_repository(dmt_api_repository_v4)} | C];
init_per_group(migration_to_v4, C) ->
    [{group_apps, genlib_app:start_application_with(dmt_api, [
        {repository, dmt_api_repository_migration},
        {migration, #{
            enable  => true,
            timer   => {timeout, 0},
            timeout => 360,
            limit   => 20
        }},
        {automaton_service_url, "http://machinegun:8022/v1/automaton"},
        {max_cache_size, 2048} % 2Kb
    ])} | C];
init_per_group(_, C) ->
    C.

start_with_repository(Repository) ->
    genlib_app:start_application_with(dmt_api, [
        {repository, Repository},
        {automaton_service_url, "http://machinegun:8022/v1/automaton"},
        {max_cache_size, 2048} % 2Kb
    ]).

-spec end_per_group(group_name(), config()) -> term().
end_per_group(Group, C) when
    Group =:= basic_lifecycle_v3 orelse
    Group =:= basic_lifecycle_v4 orelse
    Group =:= migration_to_v4
->
    genlib_app:stop_unload_applications(?config(group_apps, C));
end_per_group(_, _C) ->
    ok.

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

-spec pull_commit(term()) -> term().
pull_commit(_C) ->
    ID = next_id(),
    History1 = #{} = dmt_client_api:pull(0),
    Version1 = lists:max([0 | maps:keys(History1)]),
    Object = fixture_domain_object(ID, <<"PullFixture">>),
    Commit = #'Commit'{ops = [{insert, #'InsertOp'{object = Object}}]},
    Version2 = dmt_client_api:commit(Version1, Commit),
    #{Version2 := Commit} = dmt_client_api:pull(Version1).

-spec migration_success(term()) -> term().
migration_success(_C) ->
    #'Snapshot'{version = VersionV3} = dmt_client_api:checkout({head, #'Head'{}}),
    true = VersionV3 > 0,
    VersionV4 = wait_for_migration(VersionV3, 20, 10000),
    VersionV4 = VersionV3 + 1,
    ok.

wait_for_migration(V, TriesLeft, SleepInterval) when TriesLeft > 0 ->
    timer:sleep(SleepInterval),
    ID = next_id(),
    Object = fixture_domain_object(ID, <<"MigrationCommitFixture">>),
    Commit = #'Commit'{ops = [{insert, #'InsertOp'{object = Object}}]},
    try
        dmt_client_api:commit(V, Commit)
    catch
        _Any:_Any ->
            wait_for_migration(V, TriesLeft - 1, SleepInterval)
    end;
wait_for_migration(_, _, SleepInterval) ->
    timer:sleep(SleepInterval),
    #'Snapshot'{version = Version} = dmt_client_api:checkout({head, #'Head'{}}),
    Version.

next_id() ->
    erlang:system_time(micro_seconds) band 16#7FFFFFFF.

fixture_domain_object(Ref, Data) ->
    {category, #domain_CategoryObject{
        ref = #domain_CategoryRef{id = Ref},
        data = #domain_Category{name = Data, description = Data}
    }}.

fixture_object_ref(Ref) ->
    {category, #domain_CategoryRef{id = Ref}}.

