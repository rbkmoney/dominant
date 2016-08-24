-module(dmt_api_tests_SUITE).
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
    {ok, Apps} = application:ensure_all_started(dmt_api),
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
-spec insert(term()) -> term().
insert(_C) ->
    Object = fixture_domain_object(1, <<"InsertFixture">>),
    Ref = fixture_object_ref(1),
    #'ObjectNotFound'{} = (catch dmt_api_client:checkout_object({head, #'Head'{}}, Ref)),
    #'Snapshot'{version = Version1} = dmt_api_client:checkout({head, #'Head'{}}),
    Version2 = dmt_api_client:commit(Version1, #'Commit'{ops = [{insert, #'InsertOp'{object = Object}}]}),
    #'VersionedObject'{object = Object} = dmt_api_client:checkout_object({head, #'Head'{}}, Ref),
    #'ObjectNotFound'{} = (catch dmt_api_client:checkout_object({version, Version1}, Ref)),
    #'VersionedObject'{object = Object} = dmt_api_client:checkout_object({version, Version2}, Ref).

-spec update(term()) -> term().
update(_C) ->
    Object1 = fixture_domain_object(2, <<"UpdateFixture1">>),
    Object2 = fixture_domain_object(2, <<"UpdateFixture2">>),
    Ref = fixture_object_ref(2),
    #'Snapshot'{version = Version0} = dmt_api_client:checkout({head, #'Head'{}}),
    Version1 = dmt_api_client:commit(Version0, #'Commit'{ops = [{insert, #'InsertOp'{object = Object1}}]}),
    Version2 = dmt_api_client:commit(
        Version1, 
        #'Commit'{ops = [{update, #'UpdateOp'{old_object = Object1, new_object = Object2}}]}
    ),
    #'VersionedObject'{object = Object1} = dmt_api_client:checkout_object({version, Version1}, Ref),
    #'VersionedObject'{object = Object2} = dmt_api_client:checkout_object({version, Version2}, Ref).

-spec delete(term()) -> term().
delete(_C) ->
    Object = fixture_domain_object(3, <<"DeleteFixture">>),
    Ref = fixture_object_ref(3),
    #'Snapshot'{version = Version0} = dmt_api_client:checkout({head, #'Head'{}}),
    Version1 = dmt_api_client:commit(Version0, #'Commit'{ops = [{insert, #'InsertOp'{object = Object}}]}),
    Version2 = dmt_api_client:commit(Version1, #'Commit'{ops = [{remove, #'RemoveOp'{object = Object}}]}),
    #'VersionedObject'{object = Object} = dmt_api_client:checkout_object({version, Version1}, Ref),
    #'ObjectNotFound'{} = (catch dmt_api_client:checkout_object({version, Version2}, Ref)).

fixture_domain_object(Ref, Data) ->
    {category, #'CategoryObject'{
        ref = #'CategoryRef'{id = Ref},
        data = #'Category'{name = Data, description = Data}
    }}.

fixture_object_ref(Ref) ->
    {category, #'CategoryRef'{id = Ref}}.
