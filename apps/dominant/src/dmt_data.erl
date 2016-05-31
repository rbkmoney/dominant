-module(dmt_data).
-include_lib("dmt_proto/include/dmt_domain_thrift.hrl").

%%

-export([new/0]).
-export([get_object/2]).
-export([put_object/2]).
-export([delete_object/2]).
-export([apply_operation/2]).

-export([get_schema_revision/0]).

%%

-spec new() ->
    dominant:data().

new() ->
    #{}.

-spec get_object(dominant:object_ref(), dominant:data()) ->
    dominant:object().

get_object(ObjectRef, Data) ->
    case Result = maps:find(ObjectRef, Data) of
        {ok, _Object} ->
            Result;
        error ->
            {error, object_not_found}
    end.

-spec put_object(dominant:object(), dominant:data()) ->
    dominant:data().

put_object(Object, Data) ->
    put_object(get_ref(Object), Object, Data).

put_object(ObjectRef, Object, Data) ->
    maps:put(ObjectRef, Object, Data).

-spec delete_object(dominant:object_ref(), dominant:data()) ->
    dominant:data().

delete_object(ObjectRef, Data) ->
    maps:remove(ObjectRef, Data).

-spec apply_operation(dominant:operation(), dominant:data()) ->
    {ok, dominant:data()} | {error, term()}.

apply_operation({insert, Object}, Data) ->
    ObjectRef = try_next_ref(get_ref(Object), Data),
    case get_object(ObjectRef, Data) of
        {error, object_not_found} ->
            put_object(ObjectRef, Object, Data);
        {ok, _Object} ->
            {error, conflict}
    end;
apply_operation({update, Object}, Data) ->
    ObjectRef = get_ref(Object),
    dmt_utils:map_ok(
        get_object(ObjectRef, Data),
        fun (_) -> put_object(ObjectRef, Object, Data) end
    );
apply_operation({delete, ObjectRef}, Data) ->
    dmt_utils:map_ok(
        get_object(ObjectRef, Data),
        fun (_) -> delete_object(ObjectRef, Data) end
    ).

%%

-define(is_integer_type(T),
    Type == i64 orelse Type == i32 orelse Type == i16 orelse Type == i8
).

get_ref({Name, Object}) ->
    ObjectInfo = object_info_by_name(Name),
    get_field_by_name('ref', Object, ObjectInfo).

try_next_ref(ObjectRef, Data) ->
    ObjectRefInfo = struct_info(element(1, ObjectRef)),
    case ObjectRefInfo of
        [{_N, _Req, Type, 'id', _}] when ?is_integer_type(Type) ->
            ObjectID = get_field_by_name('id', ObjectRef, ObjectRefInfo),
            case ObjectID of
                0 ->
                    set_field_by_name('id', gen_next_id(Data), ObjectRef, ObjectRefInfo);
                _ ->
                    ObjectRef
            end;
        _ ->
            ObjectRef
    end.

object_info_by_name(Name) ->
    {struct, _, TypeRef} = field_info_type(field_info_by_name(Name, struct_info('Object'))),
    struct_info(TypeRef).

gen_next_id(Data) ->
    maps:size(Data) + 1.

%%

get_field_by_name(Name, Object, StructInfo) ->
    element(1 + field_order_by_name(Name, StructInfo), Object).

set_field_by_name(Name, Value, Object, StructInfo) ->
    setelement(1 + field_order_by_name(Name, StructInfo), Object, Value).

struct_info(TypeName) when is_atom(TypeName) ->
    struct_info({dmt_domain_thrift, TypeName});
struct_info({Module, TypeName}) ->
    {struct, _, Result} = Module:struct_info(TypeName),
    Result.

field_order_by_name(Name, Fields) ->
    field_order_by_name(Name, Fields, 1).

field_order_by_name(Name, [{_N, _Req, _Type, Name, _} | _], N) ->
    N;
field_order_by_name(Name, [_ | Rest], N) ->
    field_order_by_name(Name, Rest, N + 1).

field_info_by_name(Name, Fields) ->
    lists:keyfind(Name, 4, Fields).

field_info_type({_N, _Req, Type, _Name, _}) ->
    Type.

%%

-spec get_schema_revision() -> dominant:revision().

get_schema_revision() ->
    ?REVISION.

%%

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

get_ref_test() ->
    Ref1 = #'PaymentMethodRef'{id = bank_card},
    Object1 = {payment_method,
        #'PaymentMethodObject'{
            ref = Ref1,
            data = #'PaymentMethodDefinition'{name = <<"Bank Card">>}
        }
    },
    ?assertEqual(Ref1, get_ref(Object1)).

next_ref_test() ->
    [
        ?_assertEqual(
            #'PartyRef'{id = 1},
            try_next_ref(#'PartyRef'{id = 1}, test_data())
        ),
        ?_assertEqual(
            #'PartyRef'{id = 4},
            try_next_ref(#'PartyRef'{id = 0}, test_data())
        ),
        ?_assertEqual(
            #'PaymentMethodRef'{id = bank_card},
            try_next_ref(#'PaymentMethodRef'{id = bank_card}, test_data())
        )
    ].

test_data() ->
    #{
        {payment_method, #'PaymentMethodRef'{id = bank_card}} => {payment_method,
            #'PaymentMethodObject'{
                ref = #'PaymentMethodRef'{id = bank_card},
                data = #'PaymentMethodDefinition'{name = <<"Bank Card">>}
            }
        },
        {party, #'PartyRef'{id = 1}} => {party,
            #'PartyObject'{
                ref = #'PartyRef'{id = 1},
                data = #'Party'{registered_name = <<"Groovy">>}
            }
        },
        {category, #'CategoryRef'{id = 3}} => {category,
            #'CategoryObject'{
                ref = #'CategoryRef'{id = 3},
                data = #'Category'{name = <<"drugs">>}
            }
        }
    }.

-endif.
