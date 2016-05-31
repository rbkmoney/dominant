-module(dmt_storage).

%%

-type key() :: {ref, dominant:ref()} | {data, dominant:revision()}.
-type value() :: term().
-type context() :: term().
-type options() :: map().

-export_type([key/0]).
-export_type([value/0]).
-export_type([context/0]).
-export_type([options/0]).

-callback child_spec(options()) ->
    supervisor:child_spec() | ignore.

-callback init(options()) ->
    context().
-callback read(key(), context()) ->
    {ok, value()} | {error, notfound}.
-callback write(key(), value(), context()) ->
    {ok, value()}.

%%

-export([child_spec/2]).

-export([init/2]).
-export([read/2]).
-export([write/3]).

%%

-spec child_spec(module(), options()) ->
    [supervisor:child_spec()].

child_spec(Module, Options) ->
    Module:child_spec(Options).

-spec init(module(), options()) ->
    context().

init(Module, Options) ->
    {Module, Module:init(Options)}.

-spec read(key(), {module(), context()}) ->
    {ok, value()} | {error, notfound}.

read(Key, {Module, Context}) ->
    Module:read(Key, Context).

-spec write(key(), value(), {module(), context()}) ->
    {ok, value()}.

write(Key, Value, {Module, Context}) ->
    Module:write(Key, Value, Context).
