-module(dmt_api_repository_handler).
-behaviour(woody_server_thrift_handler).

-export([handle_function/4]).

%%

-include_lib("dmsl/include/dmsl_domain_config_thrift.hrl").

-type context() :: woody_context:ctx().

-spec handle_function
    ('Commit', woody:args(), context(), woody:options()) ->
        {ok, dmt_api_repository:version()} | no_return();
    ('Checkout', woody:args(), context(), woody:options()) ->
        {ok, dmt_api_repository:snapshot()} | no_return();
    ('Pull', woody:args(), context(), woody:options()) ->
        {ok, dmt_api_repository:history()} | no_return().
handle_function('Commit', [Version, Commit], Context, Repository) ->
    case dmt_api_repository:commit(Version, Commit, Repository, Context) of
        {ok, VersionNext} ->
            {ok, VersionNext};
        {error, {operation_conflict, {conflict, {ConflictName, _} = Conflict}}} ->
            woody_error:raise(business, #'OperationConflict'{
                conflict = {ConflictName, handle_operation_conflict(Conflict)}
            });
        {error, version_not_found} ->
            woody_error:raise(business, #'VersionNotFound'{});
        {error, head_mismatch} ->
            woody_error:raise(business, #'ObsoleteCommitVersion'{})
    end;
handle_function('Checkout', [Reference], Context, Repository) ->
    case dmt_api_repository:checkout(Reference, Repository, Context) of
        {ok, Snapshot} ->
            {ok, Snapshot};
        {error, version_not_found} ->
            woody_error:raise(business, #'VersionNotFound'{})
    end;
handle_function('Pull', [Version], Context, Repository) ->
    case dmt_api_repository:pull(Version, Repository, Context) of
        {ok, History} ->
            {ok, History};
        {error, version_not_found} ->
            woody_error:raise(business, #'VersionNotFound'{})
    end.

%%
handle_operation_conflict(Conflict) ->
    case Conflict of
        {object_already_exists, Ref} ->
            #'ObjectAlreadyExistsConflict'{object_ref = Ref};
        {object_not_found, Ref} ->
            #'ObjectNotFoundConflict'{object_ref = Ref};
        {object_reference_mismatch, Ref} ->
            #'ObjectReferenceMismatchConflict'{object_ref = Ref};
        {objects_not_exist, Refs} ->
            ObjectRefs = lists:map(
                fun({Ref, ReferencedBy}) ->
                    #'NonexistantObject'{object_ref = Ref, referenced_by = ReferencedBy}
                end,
                Refs
            ),
            #'ObjectsNotExistConflict'{object_refs = ObjectRefs}
    end.
