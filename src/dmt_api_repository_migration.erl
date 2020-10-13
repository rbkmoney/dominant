-module(dmt_api_repository_migration).

-behaviour(dmt_api_repository).

-include_lib("damsel/include/dmsl_domain_config_thrift.hrl").
-include_lib("mg_proto/include/mg_proto_state_processing_thrift.hrl").

-define(NS, <<"domain-config">>).
-define(ID, <<"migration/v4_to_v5">>).
-define(DEFAULT_MIGRATION_SETTINGS, #{
    % lagre enought, that we can process butch of old events
    timeout => 360,
    % 2xBASE, maybe even less
    limit => 20,
    % make config read-only near of the migration end
    read_only_gap => 1000
}).

%% API

-export([checkout/2]).
-export([pull/2]).
-export([pull/3]).
-export([commit/3]).

%% State processor

-behaviour(dmt_api_automaton_handler).

-export([process_call/3]).
-export([process_signal/3]).

%%

-type context() :: woody_context:ctx().
-type machine() :: mg_proto_state_processing_thrift:'Machine'().

-type ref() :: dmsl_domain_config_thrift:'Reference'().
-type snapshot() :: dmt_api_repository:snapshot().
-type commit() :: dmt_api_repository:commit().

-spec checkout(ref(), context()) ->
    {ok, snapshot()}
    | {error, version_not_found}.
checkout(Ref, Context) ->
    case is_migration_finished(Context) of
        true ->
            dmt_api_repository_v5:checkout(Ref, Context);
        false ->
            dmt_api_repository_v4:checkout(Ref, Context)
    end.

-spec pull(dmt_api_repository:version(), context()) ->
    {ok, dmt_api_repository:history()}
    | {error, version_not_found}.
pull(Version, Context) ->
    pull(Version, undefined, Context).

-spec pull(dmt_api_repository:version(), dmt_api_repository:limit(), context()) ->
    {ok, dmt_api_repository:history()}
    | {error, version_not_found}.
pull(Version, Limit, Context) ->
    case is_migration_finished(Context) of
        true ->
            dmt_api_repository_v5:pull(Version, Limit, Context);
        false ->
            dmt_api_repository_v4:pull(Version, Limit, Context)
    end.

-spec commit(dmt_api_repository:version(), commit(), context()) ->
    {ok, snapshot()}
    | {error, version_not_found | migration_in_progress | {operation_error, dmt_domain:operation_error()}}.
commit(Version, Commit, Context) ->
    case is_migration_finished(Context) of
        true ->
            dmt_api_repository_v5:commit(Version, Commit, Context);
        false ->
            case is_safe_to_commit(Version, Context) of
                true ->
                    dmt_api_repository_v4:commit(Version, Commit, Context);
                false ->
                    {error, migration_in_progress}
            end
    end.

%%

-spec process_call(dmt_api_automaton_handler:call(), machine(), context()) ->
    {dmt_api_automaton_handler:response(), dmt_api_automaton_handler:events()} | no_return().
process_call(Call, #mg_stateproc_Machine{ns = ?NS, id = ?ID} = Machine, Context) ->
    process_call_(Call, Machine, Context);
process_call(Call, #mg_stateproc_Machine{ns = ?NS, id = <<"primary/v4">>} = Machine, Context) ->
    dmt_api_repository_v4:process_call(Call, Machine, Context);
process_call(Call, #mg_stateproc_Machine{ns = ?NS, id = <<"primary/v5">>} = Machine, Context) ->
    dmt_api_repository_v5:process_call(Call, Machine, Context).

-spec process_call_(dmt_api_automaton_handler:call(), machine(), context()) -> no_return().
process_call_(_Call, _Machine, _Context) ->
    %% we shouldn't get any calls while migrating
    error({migrating, migration_in_progress}).

-spec process_signal(dmt_api_automaton_handler:signal(), machine(), context()) ->
    {dmt_api_automaton_handler:action(), dmt_api_automaton_handler:aux_state(), dmt_api_automaton_handler:events()}
    | no_return().
process_signal(Signal, #mg_stateproc_Machine{ns = ?NS, id = ?ID} = Machine, Context) ->
    process_signal_(Signal, Machine, Context);
process_signal(Signal, #mg_stateproc_Machine{ns = ?NS, id = <<"primary/v4">>} = Machine, Context) ->
    dmt_api_repository_v4:process_signal(Signal, Machine, Context);
process_signal(Signal, #mg_stateproc_Machine{ns = ?NS, id = <<"primary/v5">>} = Machine, Context) ->
    dmt_api_repository_v5:process_signal(Signal, Machine, Context).

process_signal_({init, #mg_stateproc_InitSignal{}}, _Machine, _Context) ->
    start_migration();
process_signal_({timeout, #mg_stateproc_TimeoutSignal{}}, Machine, Context) ->
    continue_migration(get_aux_state(Machine), Context).

%% Migration

get_migration_settings() ->
    genlib_app:env(dmt_api, migration, ?DEFAULT_MIGRATION_SETTINGS).

is_migration_finished(Context) ->
    AuxState = get_aux_state(get_machine(Context)),
    maps:get(is_finished, AuxState).

is_safe_to_commit(Version, Context) ->
    AuxState = get_aux_state(get_machine(Context)),
    LastMigratedVersion = maps:get(version, AuxState),
    Gap = maps:get(read_only_gap, get_migration_settings()),
    % Well, I suppose it is impossible to migrate `Gap` commits until this call will end.
    LastMigratedVersion + Gap < Version.

get_machine(Context) ->
    case dmt_api_automaton_client:get_machine(?NS, ?ID, Context) of
        {ok, Machine} ->
            Machine;
        {error, #mg_stateproc_MachineNotFound{}} ->
            ok = dmt_api_automaton_client:start(?NS, ?ID, Context),
            get_machine(Context)
    end.

start_migration() ->
    %%% start migration by setting timer up
    _ = logger:info(<<"Migration started">>, []),
    {construct_set_timer_action(), set_aux_state(#{version => 0, is_finished => false}), []}.

continue_migration(#{version := Version, is_finished := true} = State, _Context) ->
    _ = logger:info(<<"Migration finished, last version: ~p">>, [Version]),
    {#mg_stateproc_ComplexAction{}, set_aux_state(State), []};
continue_migration(#{version := Version, is_finished := false} = OldState, Context) ->
    Limit = maps:get(limit, get_migration_settings()),
    _ = logger:info(<<"Migrating events from ~p to ~p">>, [Version, Version + Limit]),
    NewState =
        case dmt_api_repository_v4:pull(Version, Limit, Context) of
            {ok, History} when map_size(History) > 0 ->
                OldState#{version => try_migrate_history(Version, History, Context)};
            {ok, _EmptyHistory} ->
                OldState#{is_finished := true}
        end,
    {construct_set_timer_action(), set_aux_state(NewState), []}.

try_migrate_history(Version, History, Context) ->
    %% TODO abstraction leak
    NextVersion = Version + 1,
    case maps:get(NextVersion, History, undefined) of
        #'Commit'{} = Commit ->
            MigratedCommit = migrate_commit(Commit),
            {ok, #'Snapshot'{version = NextVersion}} = dmt_api_repository_v5:commit(Version, MigratedCommit, Context),
            %% continue history traversing
            try_migrate_history(NextVersion, History, Context);
        undefined ->
            Version
    end.

construct_set_timer_action() ->
    MigrationSettings = get_migration_settings(),
    #mg_stateproc_ComplexAction{
        timer =
            {set_timer, #mg_stateproc_SetTimerAction{
                timer = {timeout, 0},
                range = #mg_stateproc_HistoryRange{},
                timeout = maps:get(timeout, MigrationSettings)
            }}
    }.

set_aux_state(AuxState) ->
    FmtVsn = 1,
    #mg_stateproc_Content{format_version = FmtVsn, data = encode_aux_state(FmtVsn, AuxState)}.

encode_aux_state(1, #{version := Version, is_finished := IsFinished}) ->
    {obj, #{
        {str, <<"version">>} => {i, Version},
        {str, <<"is_finished">>} => {b, IsFinished}
    }}.

get_aux_state(#mg_stateproc_Machine{aux_state = #mg_stateproc_Content{format_version = Version, data = AuxState}}) ->
    decode_aux_state(Version, AuxState).

decode_aux_state(
    1,
    {obj, #{
        {str, <<"version">>} := {i, Version},
        {str, <<"is_finished">>} := {b, IsFinished}
    }}
) ->
    #{version => Version, is_finished => IsFinished}.

migrate_commit(#'Commit'{ops = Ops} = Commit) ->
    UpdatedOps = lists:map(fun rewrite_op/1, Ops),
    NewOps = lists:flatmap(fun add_ops/1, Ops),
    Commit#'Commit'{ops = UpdatedOps ++ NewOps}.

rewrite_op({insert, #'InsertOp'{object = Object} = Op}) ->
    {insert, Op#'InsertOp'{object = rewrite_object(Object)}};
rewrite_op({update, #'UpdateOp'{old_object = OldObject, new_object = NewObject} = Op}) ->
    {update, Op#'UpdateOp'{
        old_object = rewrite_object(OldObject),
        new_object = rewrite_object(NewObject)
    }};
rewrite_op({remove, #'RemoveOp'{object = Object} = Op}) ->
    {remove, Op#'RemoveOp'{object = rewrite_object(Object)}}.

rewrite_object({provider, #domain_ProviderObject{data = Data} = Object}) ->
    NewData = Data#domain_Provider{
        terms = #domain_ProvisionTermSet{
            payments = Data#domain_Provider.payment_terms,
            recurrent_paytools = Data#domain_Provider.recurrent_paytool_terms
        }
    },
    {provider, Object#domain_ProviderObject{data = NewData}};
rewrite_object({terminal, #domain_TerminalObject{data = Data} = Object}) ->
    NewData = Data#domain_Terminal{
        terms = #domain_ProvisionTermSet{
            payments = Data#domain_Terminal.terms_legacy
        }
    },
    {terminal, Object#domain_TerminalObject{data = NewData}};
rewrite_object({payment_institution, #domain_PaymentInstitutionObject{data = Data} = Object}) ->
    NewData = Data#domain_PaymentInstitution{
        withdrawal_providers = rewrite_provider_selector(
            Data#domain_PaymentInstitution.withdrawal_providers_legacy
        ),
        p2p_providers = rewrite_provider_selector(
            Data#domain_PaymentInstitution.p2p_providers_legacy
        )
    },
    {payment_institution, Object#domain_PaymentInstitutionObject{data = NewData}};
rewrite_object(Object) ->
    Object.

rewrite_provider_selector(undefined) ->
    undefined;
rewrite_provider_selector({value, Refs}) ->
    {value, ordsets:from_list(lists:map(fun rewrite_ref/1, Refs))};
rewrite_provider_selector({decisions, Decisions}) ->
    {decisions, lists:map(fun rewrite_provider_decision/1, Decisions)}.

rewrite_provider_decision({Name, Predicate, Selector}) ->
    {rewrite_provider_decision_name(Name), Predicate, rewrite_provider_selector(Selector)}.

rewrite_provider_decision_name(domain_WithdrawalProviderDecision) ->
    domain_ProviderDecision;
rewrite_provider_decision_name(domain_P2PProviderDecision) ->
    domain_ProviderDecision;
rewrite_provider_decision_name(Name) ->
    Name.

add_ops({insert, #'InsertOp'{object = Object0} = Op}) ->
    case maybe_clone_object(Object0) of
        {add, Object1} ->
            [{insert, Op#'InsertOp'{object = Object1}}];
        ignore ->
            []
    end;
add_ops({update, #'UpdateOp'{old_object = OldObject0, new_object = NewObject0} = Op}) ->
    case maybe_clone_object(OldObject0) of
        {add, OldObject1} ->
            {add, NewObject1} = maybe_clone_object(NewObject0),
            [{update, Op#'UpdateOp'{old_object = OldObject1, new_object = NewObject1}}];
        ignore ->
            []
    end;
add_ops({remove, #'RemoveOp'{object = Object0} = Op}) ->
    case maybe_clone_object(Object0) of
        {add, Object1} ->
            [{remove, Op#'RemoveOp'{object = Object1}}];
        ignore ->
            []
    end.

maybe_clone_object({withdrawal_provider, Object}) ->
    #domain_WithdrawalProviderObject{data = Data, ref = Ref} = Object,
    NewData = #domain_Provider{
        name = Data#domain_WithdrawalProvider.name,
        description = default(Data#domain_WithdrawalProvider.description, <<"">>),
        proxy = Data#domain_WithdrawalProvider.proxy,
        identity = Data#domain_WithdrawalProvider.identity,
        accounts = Data#domain_WithdrawalProvider.accounts,
        terms = #domain_ProvisionTermSet{
            wallet = #domain_WalletProvisionTerms{
                withdrawals = Data#domain_WithdrawalProvider.withdrawal_terms
            }
        }
    },
    NewRef = rewrite_ref(Ref),
    {add, {provider, #domain_ProviderObject{data = NewData, ref = NewRef}}};
maybe_clone_object({p2p_provider, Object}) ->
    #domain_P2PProviderObject{data = Data, ref = Ref} = Object,
    NewData = #domain_Provider{
        name = Data#domain_P2PProvider.name,
        description = default(Data#domain_P2PProvider.description, <<"">>),
        proxy = Data#domain_P2PProvider.proxy,
        identity = Data#domain_P2PProvider.identity,
        accounts = Data#domain_P2PProvider.accounts,
        terms = #domain_ProvisionTermSet{
            wallet = #domain_WalletProvisionTerms{
                p2p = Data#domain_P2PProvider.p2p_terms
            }
        }
    },
    NewRef = rewrite_ref(Ref),
    {add, {provider, #domain_ProviderObject{data = NewData, ref = NewRef}}};
maybe_clone_object({cash_register_provider, Object}) ->
    #domain_CashRegisterProviderObject{data = Data, ref = Ref} = Object,
    NewData = #domain_Provider{
        name = Data#domain_CashRegisterProvider.name,
        description = default(Data#domain_CashRegisterProvider.description, <<"">>),
        proxy = Data#domain_CashRegisterProvider.proxy,
        params_schema = Data#domain_CashRegisterProvider.params_schema
    },
    NewRef = rewrite_ref(Ref),
    {add, {provider, #domain_ProviderObject{data = NewData, ref = NewRef}}};
maybe_clone_object(_Object) ->
    ignore.

rewrite_ref(#domain_WithdrawalProviderRef{id = ID}) ->
    #domain_ProviderRef{id = ID + 300};
rewrite_ref(#domain_P2PProviderRef{id = ID}) ->
    #domain_ProviderRef{id = ID + 400};
rewrite_ref(#domain_CashRegisterProviderRef{id = ID}) ->
    #domain_ProviderRef{id = ID + 450}.

default(undefined, Default) ->
    Default;
default(Value, _Default) ->
    Value.
