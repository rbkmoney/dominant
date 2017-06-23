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
        {error, {operation_conflict, {conflict, ConflictDescription}}} ->
            Conflict = case ConflictDescription of
                {object_already_exists = Error, Object} ->
                    {Error, #'ObjectAlreadyExistsConflict'{object = Object}};
                {object_not_found = Error, Ref} ->
                    {Error, #'ObjectNotFoundConflict'{object_ref = Ref}};
                {object_reference_mismatch = Error, Ref} ->
                    {Error, #'ObjectReferenceMismatchConflict'{object_ref = Ref}};
                head_mismatch = Error ->
                    {Error, #'HeadMismatchConflict'{}}
            end,
            woody_error:raise(business, #'OperationConflict'{conflict = Conflict});
        {error, version_not_found} ->
            woody_error:raise(business, #'VersionNotFound'{})
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
