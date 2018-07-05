-module(dmt_api_repository_migration).
-behaviour(dmt_api_repository).

-include_lib("dmsl/include/dmsl_domain_config_thrift.hrl").
-include_lib("mg_proto/include/mg_proto_state_processing_thrift.hrl").

-define(NS  , <<"domain-config">>).
-define(ID  , <<"migration/v3_to_v4">>).
-define(DEFAULT_MIGRATION_SETTINGS, #{
    timeout => 360, % lagre enought, that we can process butch of old events
    limit   => 20   % 2xBASE, maybe even less
}).


%% API

-export([checkout/2]).
-export([pull/2]).
-export([commit/3]).

%% State processor

-behaviour(dmt_api_automaton_handler).

-export([process_call/3]).
-export([process_signal/3]).

%%

-type context()         :: woody_context:ctx().
-type machine()         :: mg_proto_state_processing_thrift:'Machine'().

-type ref()             :: dmsl_domain_config_thrift:'Reference'().
-type snapshot()        :: dmt_api_repository:snapshot().
-type commit()          :: dmt_api_repository:commit().

-spec checkout(ref(), context()) ->
    {ok, snapshot()} |
    {error, version_not_found}.

checkout(Ref, Context) ->
    case is_migration_finished(Context) of
        true ->
            dmt_api_repository_v4:checkout(Ref, Context);
        false ->
            dmt_api_repository_v3:checkout(Ref, Context)
    end.

-spec pull(dmt_api_repository:version(), context()) ->
    {ok, dmt_api_repository:history()} |
    {error, version_not_found}.

pull(Version, Context) ->
    case is_migration_finished(Context) of
        true ->
            dmt_api_repository_v4:pull(Version, Context);
        false ->
            dmt_api_repository_v3:pull(Version, Context)
    end.

-spec commit(dmt_api_repository:version(), commit(), context()) ->
    {ok, snapshot()} |
    {error, version_not_found | {operation_conflict, dmt_api_repository:operation_conflict()}}.

commit(Version, Commit, Context) ->
    case is_migration_finished(Context) of
        true ->
            dmt_api_repository_v4:commit(Version, Commit, Context);
        false ->
            error({migrating, migration_in_progress})
    end.

%%

-spec process_call(dmt_api_automaton_handler:call(), machine(), context()) ->
    {dmt_api_automaton_handler:response(), dmt_api_automaton_handler:events()} | no_return().

process_call(
    Call,
    #mg_stateproc_Machine{ns = ?NS, id = ?ID} = Machine,
    Context
) ->
    process_call_(Call, Machine, Context);
process_call(Call, Machine, Context) ->
    % This is for v4 proccessor
    dmt_api_repository_v4:process_call(Call, Machine, Context).

-spec process_call_(dmt_api_automaton_handler:call(), machine(), context()) -> no_return().
process_call_(_Call, _Machine, _Context) ->
    %% we shouldn't get any calls while migrating
    error({migrating, migration_in_progress}).

-spec process_signal(dmt_api_automaton_handler:signal(), machine(), context()) ->
    {dmt_api_automaton_handler:action(), dmt_api_automaton_handler:aux_state(), dmt_api_automaton_handler:events()} |
    no_return().

process_signal(
    Signal,
    #mg_stateproc_Machine{ns = ?NS, id = ?ID} = Machine,
    Context
) ->
    process_signal_(Signal, Machine, Context);
process_signal(Signal, Machine, Context) ->
    % This is for v4 proccessor
    dmt_api_repository_v4:process_signal(Signal, Machine, Context).

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
    _ = lager:info(<<"Migration started">>, []),
    {construct_set_timer_action(), encode_aux_state(#{version => 0, is_finished => false}), []}.

continue_migration(#{version := Version, is_finished := true} = State, _Context) ->
    _ = lager:info(<<"Migration finished, last version: ~p">>, [Version]),
    {#mg_stateproc_ComplexAction{}, encode_aux_state(State), []};
continue_migration(#{version := Version, is_finished := false} = OldState, Context) ->
    Limit = maps:get(limit, get_migration_settings()),
    _ = lager:info(<<"Migrating events from ~p to ~p">>, [Version, Version + Limit]),
    NewState = case dmt_api_repository_v3:pull(Version, Limit, Context) of
        {ok, History} when map_size(History) > 0 ->
            OldState#{version => try_commit_history(Version, History, Context)};
        {ok, _EmptyHistory} ->
            OldState#{is_finished := true};
        {error, Error} ->
            %% this shouldn't happen, abort mission
            error({continue_migration, Error})
    end,
    {construct_set_timer_action(), encode_aux_state(NewState), []}.

try_commit_history(Version, History, Context) ->
    %% TODO abstraction leak
    case maps:get(Version + 1, History, undefined) of
        #'Commit'{} = Commit ->
            try dmt_api_repository_v4:commit(Version, Commit, Context) of
                {ok, #'Snapshot'{}} ->
                    %% continue history traversing
                    try_commit_history(Version + 1, History, Context);
                {error, Error} ->
                    %% this shouldn't happen, abort mission
                    error({continue_migration, Error})
            catch
                Type:Error ->
                    _ = lager:error(
                        <<"Migration error: ~p, stacktrace: ~p">>,
                        [{Type, Error}, erlang:get_stacktrace()]
                    ),
                    %% FIXME if we get timeout here, but commit was succesfull
                    %% we will fail on next timer iteration.

                    %% return last successfully commited version
                    Version
            end;
        undefined ->
            Version
    end.

construct_set_timer_action() ->
    MigrationSettings = get_migration_settings(),
    #mg_stateproc_ComplexAction{
        timer = {set_timer, #mg_stateproc_SetTimerAction{
            timer = {timeout, 0},
            range = #mg_stateproc_HistoryRange{},
            timeout = maps:get(timeout, MigrationSettings)
        }}
    }.

get_aux_state(#mg_stateproc_Machine{aux_state = AuxState}) ->
    decode_aux_state(AuxState).

encode_aux_state(#{version := Version, is_finished := IsFinished}) ->
    {obj, #{
        {str, <<"version">>} => {i, Version},
        {str, <<"is_finished">>} => {b, IsFinished}
    }}.

decode_aux_state({obj, #{
    {str, <<"version">>} := {i, Version},
    {str, <<"is_finished">>} := {b, IsFinished}
}}) ->
    #{version => Version, is_finished => IsFinished}.
