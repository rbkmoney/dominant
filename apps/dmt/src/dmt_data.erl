-module(dmt_data).
-include_lib("dmt_proto/include/dmt_domain_thrift.hrl").

%%

-export([new/0]).
-export([apply_operations/2]).

%%

-spec new() ->
    dmt:data().

new() ->
    #{}.

-spec apply_operation(dmt:operation(), dmt:data()) ->
    {dmt:operation(), dmt:data()}.

apply_operation({insert, Key, Value}, Data) ->
    case Data of
        #{Key := _Value} ->
            throw(already_exists);
        #{} ->
            NewData = maps:put(Key, Value, Data),
            InvertedOp = {delete, Key},
            {InvertedOp, NewData}
    end;
apply_operation({update, Key, Value}, Data) ->
    case Data of
        #{Key := OldValue} ->
            NewData = maps:put(Key, Value, Data),
            InvertedOp = {update, Key, OldValue},
            {InvertedOp, NewData};
        #{} ->
            throw(not_found)
    end;
apply_operation({delete, Key}, Data) ->
    case Data of
        #{Key := Value} ->
            NewData = maps:remove(Key, Data),
            InvertedOp = {insert, Key, Value},
            {InvertedOp, NewData};
        #{} ->
            throw(not_found)
    end.

-spec apply_operations([dmt:operation()], dmt:data()) ->
    {[dmt:operation()], dmt:data()}.

apply_operations(Ops, Data) ->
    apply_operations(Ops, [], Data).

-spec apply_operations([dmt:operation()], [dmt:operation()], dmt:data()) ->
    {[dmt:operation()], dmt:data()}.

apply_operations([], InvertedOps, Data) ->
    {InvertedOps, Data};
apply_operations([Op | Rest], InvertedOps, Data) ->
    {InvertedOp, NewData} = apply_operation(Op, Data),
    apply_operations(Rest, [InvertedOp | InvertedOps], NewData).
