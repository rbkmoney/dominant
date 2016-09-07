-module(dmt_api_context).

-type context() :: woody_client:context().

-export([new/0]).
-export([map/2]).

-spec new() -> woody_client:context().

new() ->
    ReqID = woody_client:make_unique_int(),
    woody_client:new_context(genlib:to_binary(ReqID), dmt_api_woody_event_handler).

-spec map({T1, context()}, fun((T1) -> T2)) ->
    {T2, woody_client:context()} when
        T1 :: term(),
        T2 :: term().

map({T, Context}, Fun) ->
    {Fun(T), Context}.
