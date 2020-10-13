-module(dmt_api_thrift_utils).

-export([encode/3]).
-export([decode/3]).

-type thrift_type() :: term().
-type thrift_value() :: term().

-spec encode
    (binary, thrift_type(), thrift_value()) -> binary();
    (msgpack, thrift_type(), thrift_value()) -> dmsl_msgpack_thrift:'Value'().
encode(binary, Type, Value) ->
    Codec0 = thrift_strict_binary_codec:new(),
    {ok, Codec} = thrift_strict_binary_codec:write(Codec0, Type, Value),
    Data = thrift_strict_binary_codec:close(Codec),
    Data;
encode(Proto, Type, Value) ->
    {ok, Proto0} = new_protocol(Proto),
    {Proto1, ok} = thrift_protocol:write(Proto0, {Type, Value}),
    {_Proto, {ok, Data}} = thrift_protocol:close_transport(Proto1),
    Data.

-spec decode
    (binary, thrift_type(), binary()) -> thrift_value();
    (msgpack, thrift_type(), dmsl_msgpack_thrift:'Value'()) -> thrift_value().
decode(binary, Type, Data) ->
    Codec = thrift_strict_binary_codec:new(Data),
    {ok, Value, Leftovers} = thrift_strict_binary_codec:read(Codec, Type),
    <<>> = thrift_strict_binary_codec:close(Leftovers),
    Value;
decode(Proto, Type, Data) ->
    {ok, Proto0} = new_protocol(Proto, Data),
    {_Proto, {ok, Value}} = thrift_protocol:read(Proto0, Type),
    Value.

%%

new_protocol(binary) ->
    {ok, Trans} = thrift_membuffer_transport:new(),
    thrift_binary_protocol:new(Trans, [{strict_read, true}, {strict_write, true}]);
new_protocol(msgpack) ->
    dmt_api_thrift_msgpack_protocol:new().

new_protocol(binary, Data) ->
    {ok, Trans} = thrift_membuffer_transport:new(Data),
    thrift_binary_protocol:new(Trans);
new_protocol(msgpack, Data) ->
    dmt_api_thrift_msgpack_protocol:new(Data).
