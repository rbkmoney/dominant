-module(dmt_domain).
-include_lib("dmt_proto/include/dmt_domain_config_thrift.hrl").

%%

-export([new/0]).
-export([get_object/2]).
-export([apply_operations/2]).
-export([revert_operations/2]).

%%

-spec new() ->
    dmt:domain().

new() ->
    #{}.

-spec get_object(dmt:object_ref(), dmt:domain()) ->
    dmt:domain_object().

get_object(ObjectReference, Domain) ->
    case maps:find(ObjectReference, Domain) of
        {ok, Object} ->
            Object;
        error ->
            throw(object_not_found)
    end.


-spec apply_operations([dmt:operation()], dmt:domain()) -> dmt:domain().
apply_operations([], Domain) ->
    Domain;
apply_operations([{insert, #'InsertOp'{object = Object}} | Rest], Domain) ->
    apply_operations(Rest, insert(Object, Domain));
apply_operations([{update, #'UpdateOp'{old_object = OldObject, new_object = NewObject}} | Rest], Domain) ->
    apply_operations(Rest, update(OldObject, NewObject, Domain));
apply_operations([{remove, #'DeleteOp'{object = Object}} | Rest], Domain) ->
    apply_operations(Rest, delete(Object, Domain)).

-spec revert_operations([dmt:operation()], dmt:domain()) -> dmt:domain().
revert_operations([], Domain) ->
    Domain;
revert_operations([{insert, #'InsertOp'{object = Object}} | Rest], Domain) ->
    revert_operations(Rest, delete(Object, Domain));
revert_operations([{update, #'UpdateOp'{old_object = OldObject, new_object = NewObject}} | Rest], Domain) ->
    revert_operations(Rest, update(NewObject, OldObject, Domain));
revert_operations([{remove, #'DeleteOp'{object = Object}} | Rest], Domain) ->
    revert_operations(Rest, insert(Object, Domain)).

-spec insert(dmt:domain_object(), dmt:domain()) -> dmt:domain().
insert(Object, Domain) ->
    ObjectReference = get_ref(Object),
    case maps:is_key(ObjectReference, Domain) of
        false ->
            maps:put(ObjectReference, Object, Domain);
        true ->
            throw(object_already_exists)
    end.

-spec update(dmt:domain_object(), dmt:domain_object(), dmt:domain()) -> dmt:domain().
update(OldObject, NewObject, Domain) ->
    ObjectReference = get_ref(OldObject),
    ObjectReference = get_ref(NewObject),
    case maps:find(ObjectReference, Domain) of
        {ok, OldObject} ->
            maps:put(ObjectReference, NewObject, Domain);
        error ->
            throw(object_not_found)
    end.

-spec delete(dmt:domain_object(), dmt:domain()) -> dmt:domain().
delete(Object, Domain) ->
    ObjectReference = get_ref(Object),
    case maps:find(ObjectReference, Domain) of
        {ok, Object} ->
            maps:remove(ObjectReference, Domain);
        error ->
            throw(object_not_found)
    end.

%%TODO:elaborate
-spec get_ref(dmt:domain_object()) -> dmt:object_ref().
get_ref({Tag, {_Type, Ref, _Data}}) ->
    {Tag, Ref}.