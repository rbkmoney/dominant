-module(dmt_api_tests_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0]).
-export([groups/0]).
-export([init_per_suite/1]).
-export([end_per_suite/1]).
-export([init_per_group/2]).
-export([end_per_group/2]).
-export([init_per_testcase/2]).
-export([end_per_testcase/2]).

-export([pull_commit/1]).
-export([retry_commit/1]).
-export([insert/1]).
-export([update/1]).
-export([delete/1]).
-export([migration_success/1]).
-export([conflict/1]).
-export([nonexistent/1]).
-export([reference_cycles/1]).

-include_lib("damsel/include/dmsl_domain_config_thrift.hrl").

%% tests descriptions

-type config() :: [{atom(), term()}].

-define(config(Key, C), (element(2, lists:keyfind(Key, 1, C)))).
-define(DEFAULT_LIMIT, 9001). % to emulate unlimited polling

-type test_case_name() :: atom().
-type group_name() :: atom().

-spec all() -> [{group, group_name()}].
all() ->
    [
        {group, basic_lifecycle_v3},
        {group, basic_lifecycle_v4},
        {group, migration_to_v5},
        {group, basic_lifecycle_v5}
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
            {group, basic_lifecycle},
            % {group, error_mapping},
            retry_commit
        ]},
        {basic_lifecycle_v5, [sequence], [
            pull_commit,
            {group, basic_lifecycle},
            % {group, error_mapping},
            retry_commit
        ]},
        {basic_lifecycle, [sequence, {repeat, 10}, shuffle], [
            insert,
            update,
            delete
        ]},
        {migration_to_v5, [sequence], [
            migration_success
        ]},
        {error_mapping, [parallel], [
            conflict,
            nonexistent,
            reference_cycles
        ]}
    ].

%%
%% starting/stopping
-spec init_per_suite(config()) -> config().
init_per_suite(C) ->
    Apps = genlib_app:start_application_with(scoper, [
        {storage, scoper_storage_logger}
    ]),
    [{suite_apps, Apps} | C].

-spec end_per_suite(config()) -> term().
end_per_suite(C) ->
    genlib_app:stop_unload_applications(?config(suite_apps, C)).

-spec init_per_group(group_name(), config()) -> config().
init_per_group(basic_lifecycle_v3, C) ->
    [{group_apps, start_with_repository(dmt_api_repository_v3) ++ start_client()} | C];
init_per_group(basic_lifecycle_v4, C) ->
    [{group_apps, start_with_repository(dmt_api_repository_v4) ++ start_client()} | C];
init_per_group(basic_lifecycle_v5, C) ->
    [{group_apps, start_with_repository(dmt_api_repository_v5) ++ start_client()} | C];
init_per_group(migration_to_v5, C) ->
    ApiApps = genlib_app:start_application_with(dmt_api, [
        {repository, dmt_api_repository_migration},
        {migration, #{
            timeout => 360,
            limit   => 20
        }},
        {services, #{
            automaton => #{
                url =>"http://machinegun:8022/v1/automaton"
            }
        }},
        {max_cache_size, 2048} % 2Kb
    ]),
    [{group_apps, ApiApps ++ start_client()} | C];
init_per_group(_, C) ->
    C.

start_with_repository(Repository) ->
    genlib_app:start_application_with(dmt_api, [
        {repository, Repository},
        {services, #{
            automaton => #{
                url =>"http://machinegun:8022/v1/automaton"
            }
        }},
        {max_cache_size, 52428800} % 50Mb
    ]).

start_client() ->
    genlib_app:start_application_with(dmt_client, [
        {cache_update_interval, 5000}, % milliseconds
        {cache_update_pull_limit, ?DEFAULT_LIMIT},
        {max_cache_size, #{
            elements => 20,
            memory => 52428800 % 50Mb
        }},
        {service_urls, #{
            'Repository' => <<"http://dominant:8022/v1/domain/repository">>,
            'RepositoryClient' => <<"http://dominant:8022/v1/domain/repository_client">>
        }}
    ]).

-spec end_per_group(group_name(), config()) -> term().
end_per_group(Group, C) when
    Group =:= basic_lifecycle_v3 orelse
    Group =:= basic_lifecycle_v4 orelse
    Group =:= basic_lifecycle_v5 orelse
    Group =:= migration_to_v5
->
    genlib_app:stop_unload_applications(?config(group_apps, C));
end_per_group(_, _C) ->
    ok.

-spec init_per_testcase(test_case_name(), config()) -> config().
init_per_testcase(_, C) ->
    %% added because dmt_client:checkout({head, #'Head'{}})
    %% could return old version from cache overwise
    {ok, _Version} = dmt_client_cache:update(),
    C.

-spec end_per_testcase(test_case_name(), config()) -> term().
end_per_testcase(_, _) ->
    ok.
%%
%% tests

-spec insert(term()) -> term().
insert(_C) ->
    ID = next_id(),
    Object = fixture_domain_object(ID, <<"InsertFixture">>),
    Ref = fixture_object_ref(ID),
    #'ObjectNotFound'{} = (catch dmt_client:checkout_object({head, #'Head'{}}, Ref)),
    #'Snapshot'{version = Version1} = dmt_client:checkout({head, #'Head'{}}),
    Version2 = dmt_client:commit(Version1, #'Commit'{ops = [{insert, #'InsertOp'{object = Object}}]}),
    _ = dmt_client_cache:update(),
    #'VersionedObject'{object = Object} = dmt_client:checkout_object({head, #'Head'{}}, Ref),
    #'ObjectNotFound'{} = (catch dmt_client:checkout_object({version, Version1}, Ref)),
    #'VersionedObject'{object = Object} = dmt_client:checkout_object({version, Version2}, Ref).

-spec update(term()) -> term().
update(_C) ->
    ID = next_id(),
    Object1 = fixture_domain_object(ID, <<"UpdateFixture1">>),
    Object2 = fixture_domain_object(ID, <<"UpdateFixture2">>),
    Ref = fixture_object_ref(ID),
    #'Snapshot'{version = Version0} = dmt_client:checkout({head, #'Head'{}}),
    Version1 = dmt_client:commit(Version0, #'Commit'{ops = [{insert, #'InsertOp'{object = Object1}}]}),
    Version2 = dmt_client:commit(
        Version1,
        #'Commit'{ops = [{update, #'UpdateOp'{old_object = Object1, new_object = Object2}}]}
    ),
    _ = dmt_client_cache:update(),
    #'VersionedObject'{object = Object1} = dmt_client:checkout_object({version, Version1}, Ref),
    #'VersionedObject'{object = Object2} = dmt_client:checkout_object({version, Version2}, Ref).

-spec delete(term()) -> term().
delete(_C) ->
    ID = next_id(),
    Object = fixture_domain_object(ID, <<"DeleteFixture">>),
    Ref = fixture_object_ref(ID),
    #'Snapshot'{version = Version0} = dmt_client:checkout({head, #'Head'{}}),
    Version1 = dmt_client:commit(Version0, #'Commit'{ops = [{insert, #'InsertOp'{object = Object}}]}),
    Version2 = dmt_client:commit(Version1, #'Commit'{ops = [{remove, #'RemoveOp'{object = Object}}]}),
    _ = dmt_client_cache:update(),
    #'VersionedObject'{object = Object} = dmt_client:checkout_object({version, Version1}, Ref),
    #'ObjectNotFound'{} = (catch dmt_client:checkout_object({version, Version2}, Ref)).

-spec pull_commit(term()) -> term().
pull_commit(_C) ->
    ID = next_id(),
    History1 = #{} = dmt_client:pull_range(0, ?DEFAULT_LIMIT),
    Version1 = lists:max([0 | maps:keys(History1)]),
    Object = fixture_domain_object(ID, <<"PullFixture">>),
    Commit = #'Commit'{ops = [{insert, #'InsertOp'{object = Object}}]},
    Version2 = dmt_client:commit(Version1, Commit),
    #{Version2 := Commit} = dmt_client:pull_range(Version1, ?DEFAULT_LIMIT).

-spec retry_commit(term()) -> term().
retry_commit(_C) ->
    Commit1 = #'Commit'{ops = [{insert, #'InsertOp'{
        object = fixture_domain_object(next_id(), <<"RetryCommitFixture">>)
    }}]},
    #'Snapshot'{version = Version1} = dmt_client:checkout({head, #'Head'{}}),
    Version2 = dmt_client:commit(Version1, Commit1),
    Version2 = Version1 + 1,
    Version2 = dmt_client:commit(Version1, Commit1),
    _ = dmt_client_cache:update(),
    #'Snapshot'{version = Version2} = dmt_client:checkout({head, #'Head'{}}),
    Commit2 = #'Commit'{ops = [{insert, #'InsertOp'{
        object = fixture_domain_object(next_id(), <<"RetryCommitFixture">>)
    }}]},
    Version3 = dmt_client:commit(Version2, Commit2),
    Version3 = Version2 + 1,
    Version2 = dmt_client:commit(Version1, Commit1),
    _ = dmt_client_cache:update(),
    #'Snapshot'{version = Version3} = dmt_client:checkout({head, #'Head'{}}).

-spec migration_success(term()) -> term().
migration_success(_C) ->
    #'Snapshot'{version = VersionV3} = dmt_client:checkout({head, #'Head'{}}),
    true = VersionV3 > 0,
    VersionV4 = wait_for_migration(VersionV3, 20, 1000),
    VersionV4 = VersionV3 + 1.

wait_for_migration(V, TriesLeft, SleepInterval) when TriesLeft > 0 ->
    ID = next_id(),
    Object = fixture_domain_object(ID, <<"MigrationCommitFixture">>),
    Commit = #'Commit'{ops = [{insert, #'InsertOp'{object = Object}}]},
    try
        dmt_client:commit(V, Commit)
    catch
        _Class:_Reason ->
            timer:sleep(SleepInterval),
            wait_for_migration(V, TriesLeft - 1, SleepInterval)
    end;
wait_for_migration(_, _, _) ->
    error(wait_for_migration_failed).

-spec conflict(term()) -> term().
conflict(_C) ->
    #'Snapshot'{version = Version1} = dmt_client:checkout({head, #'Head'{}}),
    _ = ?assertThrow(
        #'OperationConflict'{conflict =
            {object_not_found, #'ObjectNotFoundConflict'{
                object_ref = {criterion, #domain_CriterionRef{id = 42}}
            }}
        },
        dmt_client:commit(Version1, #'Commit'{ops = [
            {update, #'UpdateOp'{
                old_object = criterion_w_refs(42, []),
                new_object = criterion_w_refs(42, [43, 44, 45])
            }}
        ]})
    ).

-spec nonexistent(term()) -> term().
nonexistent(_C) ->
    #'Snapshot'{version = Version1} = dmt_client:checkout({head, #'Head'{}}),
    _ = ?assertThrow(
        #'OperationInvalid'{errors = [
            {object_not_exists, #'NonexistantObject'{
                object_ref = {criterion, #domain_CriterionRef{}},
                referenced_by = [{criterion, #domain_CriterionRef{id = 42}}]
            }} | _
        ]},
        dmt_client:commit(Version1, #'Commit'{ops = [
            {insert, #'InsertOp'{object = criterion_w_refs(42, [43, 44, 45])}}
        ]})
    ).

-spec reference_cycles(term()) -> term().
reference_cycles(_C) ->
    #'Snapshot'{version = Version1} = dmt_client:checkout({head, #'Head'{}}),
    _ = ?assertThrow(
        #'OperationInvalid'{errors = [
            %% we expect 3 cycles to be found
            {object_reference_cycle, #'ObjectReferenceCycle'{
                cycle = [{criterion, #domain_CriterionRef{}} | _]
            }},
            {object_reference_cycle, #'ObjectReferenceCycle'{
                cycle = [{criterion, #domain_CriterionRef{}} | _]
            }},
            {object_reference_cycle, #'ObjectReferenceCycle'{
                cycle = [{criterion, #domain_CriterionRef{}} | _]
            }}
        ]},
        dmt_client:commit(Version1, #'Commit'{ops = [
            {insert, #'InsertOp'{object = criterion_w_refs(1, [2])}},
            {insert, #'InsertOp'{object = criterion_w_refs(2, [3])}},
            {insert, #'InsertOp'{object = criterion_w_refs(3, [4, 1])}},
            {insert, #'InsertOp'{object = criterion_w_refs(4, [1, 2])}}
        ]})
    ).

next_id() ->
    erlang:system_time(micro_seconds) band 16#7FFFFFFF.

fixture_domain_object(Ref, Data) ->
    {category, #domain_CategoryObject{
        ref = #domain_CategoryRef{id = Ref},
        data = #domain_Category{name = Data, description = Data}
    }}.

fixture_object_ref(Ref) ->
    {category, #domain_CategoryRef{id = Ref}}.

criterion_w_refs(ID, Refs) ->
    {criterion, #domain_CriterionObject{
        ref = #domain_CriterionRef{id = ID},
        data = #domain_Criterion{
            name = genlib:format(ID),
            predicate = {any_of,
                ordsets:from_list([{criterion, #domain_CriterionRef{id = Ref}} || Ref <- Refs])
            }
        }
    }}.
