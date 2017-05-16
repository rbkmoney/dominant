-module(dmt_api_thrift_msgpack_protocol).

%%% Thrift library uses legacy behaviour definition that makes dialyzer angry.
%%% -behaviour(thrift_protocol).

-include_lib("thrift/include/thrift_constants.hrl").
-include_lib("thrift/include/thrift_protocol.hrl").

-export([
    new/0,
    new/1,
    read/2,
    write/2,
    flush_transport/1,
    close_transport/1
]).

-record(msgpack_protocol, {
    production :: production()
}).

-type production() ::
    [] |
    value() |
    {array, [production()], value()} |
    {map, none | value(), [{value(), value()}] | #{value() => value()}, value()}.

-type value() :: dmsl_msgpack_thrift:'Value'().

-type state() :: #msgpack_protocol{}.

-include_lib("thrift/include/thrift_protocol_behaviour.hrl").

-spec new() -> term(). % FIXME

new() ->
    new([]).

-spec new(production()) -> term(). % FIXME

new(Production) ->
    State = #msgpack_protocol{production = Production},
    thrift_protocol:new(?MODULE, State).

flush_transport(This) ->
    {This, ok}.

close_transport(This = #msgpack_protocol{production = Production}) ->
    {This, {ok, Production}}.

%%%
%%% instance methods
%%%

typeid_to_string(?tType_BOOL)   -> <<"bool">>;
typeid_to_string(?tType_DOUBLE) -> <<"dbl">>;
typeid_to_string(?tType_I8)     -> <<"i8">>;
typeid_to_string(?tType_I16)    -> <<"i16">>;
typeid_to_string(?tType_I32)    -> <<"i32">>;
typeid_to_string(?tType_I64)    -> <<"i64">>;
typeid_to_string(?tType_STRING) -> <<"str">>;
typeid_to_string(?tType_STRUCT) -> <<"struct">>;
typeid_to_string(?tType_MAP)    -> <<"map">>;
typeid_to_string(?tType_SET)    -> <<"set">>;
typeid_to_string(?tType_LIST)   -> <<"list">>.

-define(str(V)  , {str , V}).
-define(int(V)  , {  i , V}).
-define(flt(V)  , {flt , V}).
-define(bool(V) , {  b , V}).

write(This, #protocol_message_begin{name = Name, type = Type, seqid = Seqid}) ->
    do_write_many(This, [
        {enter, array},
        ?str(Name),
        ?int(Type),
        ?int(Seqid)
    ]);
write(This, message_end) ->
    do_write(This, {exit, array});

write(This, #protocol_struct_begin{}) ->
    do_write(This, {enter, map});
write(This, struct_end) ->
    {This, ok};

write(This, #protocol_field_begin{name = Name, type = Type, id = Id}) ->
    do_write_many(This, [
        ?str(genlib:to_binary(Name)),
        {enter, array},
        ?int(Id),
        ?str(typeid_to_string(Type))
    ]);
write(This, field_end) ->
    do_write(This, {exit, array});

write(This, field_stop) ->
    do_write(This, {exit, map});

write(This, #protocol_map_begin{ktype = Ktype, vtype = Vtype, size = Size}) ->
    do_write_many(This, [
        {enter, array},
        {enter, map},
        ?str(typeid_to_string(Ktype)),
        ?str(typeid_to_string(Vtype)),
        {exit, map},
        ?int(Size),
        {enter, map}
    ]);
write(This, map_end) ->
    do_write_many(This, [
        {exit, map},
        {exit, array}
    ]);

write(This, #protocol_list_begin{etype = Etype, size = Size}) ->
    do_write_many(This, [
        {enter, array},
        ?str(typeid_to_string(Etype)),
        ?int(Size)
    ]);
write(This, list_end) ->
    do_write(This, {exit, array});

write(This, #protocol_set_begin{etype = Etype, size = Size}) ->
    write(This, #protocol_list_begin{etype = Etype, size = Size});
write(This, set_end) ->
    write(This, list_end);

write(This, {bool, V}) ->
    do_write(This, ?bool(V));

write(This, {byte, Byte}) ->
    do_write(This, ?int(Byte));
write(This, {i16, I16}) ->
    do_write(This, ?int(I16));
write(This, {i32, I32}) ->
    do_write(This, ?int(I32));
write(This, {i64, I64}) ->
    do_write(This, ?int(I64));

write(This, {double, Double}) ->
    do_write(This, ?flt(Double));

write(This, {string, Bin}) when is_binary(Bin) ->
    % FIXME nonprintable?
    do_write(This, ?str(Bin)).

%%

do_write_many(#msgpack_protocol{production = Production}, Vs) ->
    {#msgpack_protocol{production = write_vals(Production, Vs)}, ok}.

do_write(#msgpack_protocol{production = Production}, V) ->
    {#msgpack_protocol{production = write_val(Production, V)}, ok}.

write_vals(P0, [V | Vs]) ->
    write_vals(write_val(P0, V), Vs);
write_vals(P0, []) ->
    P0.

write_val(Production, {enter, array}) ->
    {array, [], Production};
write_val({array, Vs, Production}, {exit, array}) ->
    write_val(Production, {arr, lists:reverse(Vs)});

write_val(Production, {enter, map}) ->
    {map, none, #{}, Production};
write_val({map, none, Vs, Production}, {exit, map}) ->
    write_val(Production, {obj, Vs});

write_val({array, Vs, Production}, V) ->
    {array, [V | Vs], Production};

write_val({map, none, Vs, Production}, K) ->
    {map, K, Vs, Production};
write_val({map, K, Vs, Production}, V) ->
    {map, none, Vs#{K => V}, Production};

write_val([], V) ->
    V.

%%

string_to_typeid(<<"bool">>)    -> ?tType_BOOL;
string_to_typeid(<<"dbl">>)     -> ?tType_DOUBLE;
string_to_typeid(<<"i8">>)      -> ?tType_I8;
string_to_typeid(<<"i16">>)     -> ?tType_I16;
string_to_typeid(<<"i32">>)     -> ?tType_I32;
string_to_typeid(<<"i64">>)     -> ?tType_I64;
string_to_typeid(<<"str">>)     -> ?tType_STRING;
string_to_typeid(<<"struct">>)  -> ?tType_STRUCT;
string_to_typeid(<<"map">>)     -> ?tType_MAP;
string_to_typeid(<<"set">>)     -> ?tType_SET;
string_to_typeid(<<"list">>)    -> ?tType_LIST.

read(This, message_begin) ->
    {This1, {ok, [Name, Type, SeqId]}} = do_read_many(This, [
        {enter, array},
        str,
        int,
        int
    ]),
    {This1, #protocol_message_begin{name = binary_to_list(Name), type = Type, seqid = SeqId}};
read(This, message_end) ->
    do_read(This, {exit, array});

read(This, struct_begin) ->
    do_read(This, {enter, map});
read(This, struct_end) ->
    do_read(This, {exit, map});

read(This0, field_begin) ->
    case do_read(This0, str) of
        {This1, {ok, Name}} ->
            {This2, {ok, [Id, Type]}} = do_read_many(This1, [{enter, array}, int, str]),
            {This2, #protocol_field_begin{type = string_to_typeid(Type), id = Id, name = Name}};
        {This1, {error, object_exhausted}} ->
            {This1, #protocol_field_begin{type = ?tType_STOP}}
    end;
read(This0, field_end) ->
    do_read(This0, {exit, array});

read(This0, map_begin) ->
    {This1, {ok, [Ktype, Vtype, Size]}} = do_read_many(This0, [
        {enter, array},
        {enter, map},
        str,
        str,
        {exit, map},
        int,
        {enter, map}
    ]),
    {This1, #protocol_map_begin{
        ktype = string_to_typeid(Ktype),
        vtype = string_to_typeid(Vtype),
        size = Size
    }};
read(This, map_end) ->
    do_read_many(This, [{exit, map}, {exit, array}]);

read(This0, list_begin) ->
    {This1, {ok, [Etype, Size]}} = do_read_many(This0, [
        {enter, array},
        str,
        int
    ]),
    {This1, #protocol_list_begin{
        etype = string_to_typeid(Etype),
        size = Size
    }};
read(This0, list_end) ->
    do_read(This0, {exit, array});

read(This0, set_begin) ->
    {This1, #protocol_list_begin{etype = Etype, size = Size}} = read(This0, list_begin),
    {This1, #protocol_set_begin{etype = Etype, size = Size}};
read(This0, set_end) ->
    read(This0, list_end);

read(This0, field_stop) ->
    {This0, ok};

read(This0, bool) ->
    do_read(This0, bool);

read(This0, byte) ->
    do_read(This0, int);
read(This0, i16) ->
    do_read(This0, int);
read(This0, i32) ->
    do_read(This0, int);
read(This0, i64) ->
    do_read(This0, int);

read(This0, double) ->
    do_read(This0, flt);

read(This0, string) ->
    do_read(This0, str).

%%

do_read_many(This = #msgpack_protocol{production = P0}, Ts) ->
    {P1, Result} = do_read_many(P0, Ts, []),
    {This#msgpack_protocol{production = P1}, Result}.

do_read_many(P0, [T | Ts], Vs) ->
    case read_val(P0, T) of
        {P1, ok} ->
            do_read_many(P1, Ts, Vs);
        {P1, {ok, V}} ->
            do_read_many(P1, Ts, [V | Vs]);
        {_P, {error, _}} = Result ->
            Result
    end;
do_read_many(P0, [], []) ->
    {P0, ok};
do_read_many(P0, [], Vs) ->
    {P0, {ok, lists:reverse(Vs)}}.

do_read(#msgpack_protocol{production = P0}, T) ->
    {P1, R} = read_val(P0, T),
    {#msgpack_protocol{production = P1}, R}.

read_val(Production, {enter, array}) ->
    case read_next(Production) of
        {Rest, {arr, Vs}} ->
            {{array, Vs, Rest}, ok};
        {_Rest, {error, _} = Error} ->
            {Production, Error};
        {_Rest, {T, _V}} ->
            {Production, {error, {array_expected, T}}}
    end;
read_val({array, [], Rest}, {exit, array}) ->
    {Rest, ok};

read_val(Production, {enter, map}) ->
    case read_next(Production) of
        {Rest, {obj, Vs}} ->
            {{map, none, maps:to_list(Vs), Rest}, ok};
        {_Rest, {error, _} = Error} ->
            {Production, Error};
        {_Rest, {T, _V}} ->
            {Production, {error, {object_expected, T}}}
    end;
read_val({map, none, [], Rest}, {exit, map}) ->
    {Rest, ok};

read_val(P0, T) ->
    case read_next(P0) of
        {P1, {error, _} = Error} ->
            {P1, Error};
        {P1, V} ->
            {P1, read_scalar(V, T)}
    end.

read_next({array, [V | Vs], Rest}) ->
    {{array, Vs, Rest}, V};
read_next({array, [], _} = Production) ->
    {Production, {error, array_exhausted}};

read_next({map, none, [{K, V} | Vs], Rest}) ->
    {{map, V, Vs, Rest}, K};
read_next({map, none, [], _} = Production) ->
    {Production, {error, object_exhausted}};
read_next({map, V, Vs, Rest}) ->
    {{map, none, Vs, Rest}, V};

read_next(V) ->
    {[], V}.

read_scalar(?bool(V), bool) ->
    {ok, V};
read_scalar(?str(V), str) ->
    {ok, V};
read_scalar(?int(V), int) ->
    {ok, V};
read_scalar(?flt(V), flt) ->
    {ok, V};
read_scalar(V, T) ->
    {error, {type_mismatch, V, T}}.
