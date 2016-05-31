-module(dmt_utils).

-export_type([result/2]).

-export([map_ok/2]).
-export([map_error/2]).
-export([bind/2]).

%%

-type result(R, E) :: {ok, R} | {error, E}.

-spec map_ok(result(R1, E), fun((R1) -> R2)) -> result(R2, E) when
    R1 :: term(),
    R2 :: term(),
    E :: term().

map_ok({ok, R1}, Fun) ->
    {ok, Fun(R1)};
map_ok(E, _Fun) ->
    E.

-spec map_error(result(R, E1), fun((E1) -> E2)) -> result(R, E2) when
    R :: term(),
    E1 :: term(),
    E2 :: term().

map_error({error, E1}, Fun) ->
    {error, Fun(E1)};
map_error(R, _Fun) ->
    R.

-spec bind(result(R1, E1), fun((R1) -> result(R2, E2))) -> result(R2, E1 | E2) when
    R1 :: term(),
    R2 :: term(),
    E1 :: term(),
    E2 :: term().

bind({ok, R}, Fun) ->
    Fun(R);
bind(E, _Fun) ->
    E.
