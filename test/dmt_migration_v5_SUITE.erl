-module(dmt_migration_v5_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-include_lib("damsel/include/dmsl_domain_config_thrift.hrl").
-include_lib("mg_proto/include/mg_proto_state_processing_thrift.hrl").

-export([all/0]).
-export([groups/0]).

-export([init_per_suite/1]).
-export([end_per_suite/1]).
-export([init_per_testcase/2]).
-export([end_per_testcase/2]).

-export([provider_terms_rewriting_test/1]).
-export([terminal_terms_rewriting_test/1]).
-export([institution_provider_rewriting_test/1]).
-export([institution_provider_undefined_rewriting_test/1]).
-export([withdrawal_provider_add_test/1]).
-export([cash_reg_provider_add_test/1]).

%% tests descriptions

-type config() :: [{atom(), term()}].

-define(config(Key, C), (element(2, lists:keyfind(Key, 1, C)))).
% to emulate unlimited polling
-define(DEFAULT_LIMIT, 9001).

-type test_case_name() :: atom().
-type group_name() :: atom().

-spec all() -> [{group, group_name()}].
all() ->
    [
        {group, all}
    ].

-spec groups() -> any().
groups() ->
    [
        {all, [sequence], [
            provider_terms_rewriting_test,
            terminal_terms_rewriting_test,
            institution_provider_rewriting_test,
            institution_provider_undefined_rewriting_test,
            withdrawal_provider_add_test,
            cash_reg_provider_add_test
        ]}
    ].

-spec init_per_suite(config()) -> config().
init_per_suite(C) ->
    Apps =
        genlib_app:start_application_with(scoper, [
            {storage, scoper_storage_logger}
        ]) ++ genlib_app:start_application_with(woody, []),
    [{suite_apps, Apps} | C].

-spec end_per_suite(config()) -> term().
end_per_suite(C) ->
    ok = clean_config(),
    genlib_app:stop_unload_applications(?config(suite_apps, C)).

-spec init_per_testcase(test_case_name(), config()) -> config().
init_per_testcase(_, C) ->
    ok = clean_config(),
    C.

-spec end_per_testcase(test_case_name(), config()) -> term().
end_per_testcase(_, _) ->
    ok.

%% Tests

-spec provider_terms_rewriting_test(term()) -> term().
provider_terms_rewriting_test(_C) ->
    Apps0 = start_with_repository(dmt_api_repository_v4),
    Ref = #domain_ProviderRef{id = next_id()},
    Data0 = #domain_Provider{
        name = <<"Brovider">>,
        description = <<"Brovider description">>,
        proxy = prepare_proxy(),
        payment_terms = #domain_PaymentsProvisionTerms{},
        recurrent_paytool_terms = #domain_RecurrentPaytoolsProvisionTerms{
            cash_value =
                {value, #domain_Cash{
                    amount = 100,
                    currency = prepare_currency()
                }},
            categories = {value, []},
            payment_methods = {value, []}
        }
    },
    Object0 = {provider, #domain_ProviderObject{ref = Ref, data = Data0}},
    Object1 =
        {provider, #domain_ProviderObject{
            ref = Ref,
            data = Data0#domain_Provider{name = <<"Drovider">>}
        }},
    Version0 = insert(Object0),
    Version1 = update(Object0, Object1),
    Version2 = remove(Object1),
    {Version3, Apps1} = migrate(Version2, Apps0),
    Expected0 = #domain_ProviderObject{
        ref = Ref,
        data = Data0#domain_Provider{
            terms = #domain_ProvisionTermSet{
                payments = Data0#domain_Provider.payment_terms,
                recurrent_paytools = Data0#domain_Provider.recurrent_paytool_terms
            }
        }
    },
    Expected1 = Expected0#domain_ProviderObject{
        data = Expected0#domain_ProviderObject.data#domain_Provider{name = <<"Drovider">>}
    },
    ?assertEqual(Expected0, checkout({provider, Ref}, Version0)),
    ?assertEqual(Expected1, checkout({provider, Ref}, Version1)),
    ?assertEqual(not_found, checkout({provider, Ref}, Version2)),
    ?assertEqual(not_found, checkout({provider, Ref}, Version3)),
    ok = stop(Apps1).

-spec terminal_terms_rewriting_test(term()) -> term().
terminal_terms_rewriting_test(_C) ->
    Apps0 = start_with_repository(dmt_api_repository_v4),
    Ref = #domain_TerminalRef{id = next_id()},
    Data0 = #domain_Terminal{
        name = <<"Brominal">>,
        description = <<"Brominal description">>,
        terms = #domain_ProvisionTermSet{
            payments = #domain_PaymentsProvisionTerms{}
        }
    },
    Object0 = {terminal, #domain_TerminalObject{ref = Ref, data = Data0}},
    Object1 =
        {terminal, #domain_TerminalObject{
            ref = Ref,
            data = Data0#domain_Terminal{name = <<"Drominal">>}
        }},
    Version0 = insert(Object0),
    Version1 = update(Object0, Object1),
    Version2 = remove(Object1),
    {Version3, Apps1} = migrate(Version2, Apps0),
    Expected0 = #domain_TerminalObject{
        ref = Ref,
        data = Data0
    },
    Expected1 = Expected0#domain_TerminalObject{
        data = Expected0#domain_TerminalObject.data#domain_Terminal{name = <<"Drominal">>}
    },
    ?assertEqual(Expected0, checkout({terminal, Ref}, Version0)),
    ?assertEqual(Expected1, checkout({terminal, Ref}, Version1)),
    ?assertEqual(not_found, checkout({terminal, Ref}, Version2)),
    ?assertEqual(not_found, checkout({terminal, Ref}, Version3)),
    ok = stop(Apps1).

-spec institution_provider_rewriting_test(term()) -> term().
institution_provider_rewriting_test(_C) ->
    Apps0 = start_with_repository(dmt_api_repository_v4),
    Ref = #domain_PaymentInstitutionRef{id = next_id()},
    Data0 = #domain_PaymentInstitution{
        name = <<"Brovider">>,
        system_account_set = {value, prepare_system_account_set()},
        default_contract_template = {value, prepare_contract_template()},
        inspector = {value, prepare_inspector()},
        realm = test,
        residences = [],
        withdrawal_providers_legacy =
            {decisions, [
                #domain_WithdrawalProviderDecision{
                    if_ = {constant, true},
                    then_ = {value, [prepare_withdrawal_provider(1)]}
                }
            ]}
    },
    Object0 = {payment_institution, #domain_PaymentInstitutionObject{ref = Ref, data = Data0}},
    Version0 = insert(Object0),
    {Version1, Apps1} = migrate(Version0, Apps0),
    Expected = #domain_PaymentInstitutionObject{
        ref = Ref,
        data = Data0#domain_PaymentInstitution{
            withdrawal_providers =
                {decisions, [
                    #domain_ProviderDecision{
                        if_ = {constant, true},
                        then_ = {value, [#domain_ProviderRef{id = 301}]}
                    }
                ]}
        }
    },
    ?assertEqual(Expected, checkout({payment_institution, Ref}, Version0)),
    ?assertEqual(Expected, checkout({payment_institution, Ref}, Version1)),
    ok = stop(Apps1).

-spec institution_provider_undefined_rewriting_test(term()) -> term().
institution_provider_undefined_rewriting_test(_C) ->
    Apps0 = start_with_repository(dmt_api_repository_v4),
    Ref = #domain_PaymentInstitutionRef{id = next_id()},
    Data0 = #domain_PaymentInstitution{
        name = <<"Brovider">>,
        system_account_set = {value, prepare_system_account_set()},
        default_contract_template = {value, prepare_contract_template()},
        inspector = {value, prepare_inspector()},
        realm = test,
        residences = [],
        withdrawal_providers_legacy = undefined
    },
    Object0 = {payment_institution, #domain_PaymentInstitutionObject{ref = Ref, data = Data0}},
    Version0 = insert(Object0),
    {Version1, Apps1} = migrate(Version0, Apps0),
    Expected = #domain_PaymentInstitutionObject{
        ref = Ref,
        data = Data0#domain_PaymentInstitution{
            withdrawal_providers = undefined
        }
    },
    ?assertEqual(Expected, checkout({payment_institution, Ref}, Version0)),
    ?assertEqual(Expected, checkout({payment_institution, Ref}, Version1)),
    ok = stop(Apps1).

-spec withdrawal_provider_add_test(term()) -> term().
withdrawal_provider_add_test(_C) ->
    Apps0 = start_with_repository(dmt_api_repository_v4),
    Ref0 = #domain_WithdrawalProviderRef{id = 1},
    Data0 = #domain_WithdrawalProvider{
        name = <<"Wrovider">>,
        proxy = prepare_proxy(),
        withdrawal_terms = #domain_WithdrawalProvisionTerms{}
    },
    Object0 = #domain_WithdrawalProviderObject{ref = Ref0, data = Data0},
    Object1 = Object0#domain_WithdrawalProviderObject{
        data = Data0#domain_WithdrawalProvider{name = <<"Vrovider">>}
    },
    Version0 = insert({withdrawal_provider, Object0}),
    Version1 = update({withdrawal_provider, Object0}, {withdrawal_provider, Object1}),
    Version2 = remove({withdrawal_provider, Object1}),
    {Version3, Apps1} = migrate(Version2, Apps0),
    Ref1 = #domain_ProviderRef{id = 301},
    Expected0 = #domain_ProviderObject{
        ref = Ref1,
        data = #domain_Provider{
            name = Data0#domain_WithdrawalProvider.name,
            description = <<>>,
            proxy = Data0#domain_WithdrawalProvider.proxy,
            identity = Data0#domain_WithdrawalProvider.identity,
            accounts = Data0#domain_WithdrawalProvider.accounts,
            terms = #domain_ProvisionTermSet{
                wallet = #domain_WalletProvisionTerms{
                    withdrawals = Data0#domain_WithdrawalProvider.withdrawal_terms
                }
            }
        }
    },
    Expected1 = Expected0#domain_ProviderObject{
        data = Expected0#domain_ProviderObject.data#domain_Provider{name = <<"Vrovider">>}
    },
    ?assertEqual(Expected0, checkout({provider, Ref1}, Version0)),
    ?assertEqual(Expected1, checkout({provider, Ref1}, Version1)),
    ?assertEqual(not_found, checkout({provider, Ref1}, Version2)),
    ?assertEqual(not_found, checkout({provider, Ref1}, Version3)),
    ?assertEqual(Object0, checkout({withdrawal_provider, Ref0}, Version0)),
    ?assertEqual(Object1, checkout({withdrawal_provider, Ref0}, Version1)),
    ?assertEqual(not_found, checkout({withdrawal_provider, Ref0}, Version2)),
    ok = stop(Apps1).

-spec cash_reg_provider_add_test(term()) -> term().
cash_reg_provider_add_test(_C) ->
    Apps0 = start_with_repository(dmt_api_repository_v4),
    Ref0 = #domain_CashRegisterProviderRef{id = 1},
    Data0 = #domain_CashRegisterProvider{
        name = <<"Crovider">>,
        proxy = prepare_proxy(),
        params_schema = []
    },
    Object0 = #domain_CashRegisterProviderObject{ref = Ref0, data = Data0},
    Object1 = #domain_CashRegisterProviderObject{
        ref = Ref0,
        data = Data0#domain_CashRegisterProvider{name = <<"Srovider">>}
    },
    Version0 = insert({cash_register_provider, Object0}),
    Version1 = update({cash_register_provider, Object0}, {cash_register_provider, Object1}),
    Version2 = remove({cash_register_provider, Object1}),
    {Version3, Apps1} = migrate(Version2, Apps0),
    Ref1 = #domain_ProviderRef{id = 451},
    Expected0 = #domain_ProviderObject{
        ref = Ref1,
        data = #domain_Provider{
            name = Data0#domain_CashRegisterProvider.name,
            description = <<>>,
            proxy = Data0#domain_CashRegisterProvider.proxy,
            params_schema = Data0#domain_CashRegisterProvider.params_schema
        }
    },
    Expected1 = Expected0#domain_ProviderObject{
        data = Expected0#domain_ProviderObject.data#domain_Provider{name = <<"Srovider">>}
    },
    ?assertEqual(Expected0, checkout({provider, Ref1}, Version0)),
    ?assertEqual(Expected1, checkout({provider, Ref1}, Version1)),
    ?assertEqual(not_found, checkout({provider, Ref1}, Version2)),
    ?assertEqual(not_found, checkout({provider, Ref1}, Version3)),
    ?assertEqual(Object0, checkout({cash_register_provider, Ref0}, Version0)),
    ?assertEqual(Object1, checkout({cash_register_provider, Ref0}, Version1)),
    ?assertEqual(not_found, checkout({cash_register_provider, Ref0}, Version2)),
    ok = stop(Apps1).

%% Helpers

prepare_currency() ->
    ID = genlib:unique(),
    Ref = #domain_CurrencyRef{symbolic_code = ID},
    Object =
        {currency, #domain_CurrencyObject{
            ref = Ref,
            data = #domain_Currency{
                name = <<>>,
                symbolic_code = ID,
                numeric_code = 0,
                exponent = 1
            }
        }},
    _Version = insert(Object),
    Ref.

prepare_proxy() ->
    Ref = #domain_ProxyRef{id = next_id()},
    Object =
        {proxy, #domain_ProxyObject{
            ref = Ref,
            data = #domain_ProxyDefinition{
                name = <<>>,
                description = <<>>,
                url = <<>>,
                options = #{}
            }
        }},
    _Version = insert(Object),
    #domain_Proxy{
        ref = Ref,
        additional = #{}
    }.

prepare_inspector() ->
    Ref = #domain_InspectorRef{id = next_id()},
    Object =
        {inspector, #domain_InspectorObject{
            ref = Ref,
            data = #domain_Inspector{
                name = <<"Gadget">>,
                description = <<"Yet another inspector">>,
                proxy = prepare_proxy()
            }
        }},
    _Version = insert(Object),
    Ref.

prepare_system_account_set() ->
    Ref = #domain_SystemAccountSetRef{id = next_id()},
    Object =
        {system_account_set, #domain_SystemAccountSetObject{
            ref = Ref,
            data = #domain_SystemAccountSet{
                name = <<"Super set">>,
                description = <<"">>,
                accounts = #{}
            }
        }},
    _Version = insert(Object),
    Ref.

prepare_contract_template() ->
    Ref = #domain_ContractTemplateRef{id = next_id()},
    Object =
        {contract_template, #domain_ContractTemplateObject{
            ref = Ref,
            data = #domain_ContractTemplate{
                terms = prepare_term_set_hierarchy()
            }
        }},
    _Version = insert(Object),
    Ref.

prepare_term_set_hierarchy() ->
    Ref = #domain_TermSetHierarchyRef{id = next_id()},
    Object =
        {term_set_hierarchy, #domain_TermSetHierarchyObject{
            ref = Ref,
            data = #domain_TermSetHierarchy{
                term_sets = []
            }
        }},
    _Version = insert(Object),
    Ref.

prepare_withdrawal_provider(ID) ->
    Ref = #domain_WithdrawalProviderRef{id = ID},
    Object =
        {withdrawal_provider, #domain_WithdrawalProviderObject{
            ref = Ref,
            data = #domain_WithdrawalProvider{
                name = <<"">>,
                proxy = prepare_proxy()
            }
        }},
    _Version = insert(Object),
    Ref.

next_id() ->
    erlang:system_time(micro_seconds) band 16#7FFFFFFF.

insert(Object) ->
    dmt_client:commit(#'Commit'{ops = [{insert, #'InsertOp'{object = Object}}]}).

update(Object0, Object1) ->
    dmt_client:commit(#'Commit'{
        ops = [
            {update, #'UpdateOp'{old_object = Object0, new_object = Object1}}
        ]
    }).

remove(Object) ->
    dmt_client:commit(#'Commit'{ops = [{remove, #'RemoveOp'{object = Object}}]}).

checkout(Ref, Version) ->
    try dmt_client:checkout_object(Version, Ref) of
        {_Tag, Object} ->
            Object
    catch
        throw:#'ObjectNotFound'{} ->
            not_found;
        throw:Reason ->
            erlang:error(Reason)
    end.

start_with_repository(Repository) ->
    ServiceApps = genlib_app:start_application_with(dmt_api, [
        {repository, Repository},
        {services, #{
            automaton => #{
                url => "http://machinegun:8022/v1/automaton"
            }
        }},
        {migration, #{
            timeout => 360,
            limit => 20
        }},
        % 50Mb
        {max_cache_size, 52428800}
    ]),
    ClientApps = genlib_app:start_application_with(dmt_client, [
        % milliseconds
        {cache_update_interval, 5000},
        {cache_update_pull_limit, ?DEFAULT_LIMIT},
        {max_cache_size, #{
            elements => 20,
            % 50Mb
            memory => 52428800
        }},
        {service_urls, #{
            'Repository' => <<"http://dominant:8022/v1/domain/repository">>,
            'RepositoryClient' => <<"http://dominant:8022/v1/domain/repository_client">>
        }}
    ]),
    ServiceApps ++ ClientApps.

stop(Apps) ->
    genlib_app:stop_unload_applications(Apps).

migrate(Version0, Apps0) ->
    ok = stop(Apps0),
    Apps1 = start_with_repository(dmt_api_repository_migration),
    Version1 = wait_for_migration(Version0, 20, 1000),
    % reset dmt_client cache
    ok = stop(Apps1),
    {Version1, start_with_repository(dmt_api_repository_migration)}.

wait_for_migration(V, TriesLeft, SleepInterval) when TriesLeft > 0 ->
    Object =
        {category, #domain_CategoryObject{
            ref = #domain_CategoryRef{id = next_id()},
            data = #domain_Category{
                name = <<"MigrationCommitFixture">>,
                description = <<"MigrationCommitFixture">>
            }
        }},
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

clean_config() ->
    ok = delete_machine(<<"domain-config">>, <<"primary/v4">>),
    ok = delete_machine(<<"domain-config">>, <<"primary/v5">>),
    ok = delete_machine(<<"domain-config">>, <<"migration/v4_to_v5">>),
    ok.

delete_machine(NS, ID) ->
    Opts = #{
        url => "http://machinegun:8022/v1/automaton",
        event_handler => {scoper_woody_event_handler, #{}}
    },
    Request = {{mg_proto_state_processing_thrift, 'Automaton'}, 'Remove', {NS, ID}},
    case woody_client:call(Request, Opts, woody_context:new()) of
        {ok, ok} ->
            ok;
        {exception, #mg_stateproc_NamespaceNotFound{}} ->
            error(namespace_not_found);
        {exception, #mg_stateproc_MachineNotFound{}} ->
            ok;
        {exception, Exception} ->
            {error, Exception}
    end.
