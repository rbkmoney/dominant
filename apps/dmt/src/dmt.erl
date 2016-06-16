%%% @doc Public API, supervisor and application startup.
%%% @end

-module(dmt).
-behaviour(supervisor).
-behaviour(application).

%% API
-export([checkout/1]).
-export([checkout/2]).
-export([commit/1]).
-export([current_schema/0]).
-export([current_version/0]).

%% Supervisor callbacks
-export([init/1]).

%% Application callbacks
-export([start/2]).
-export([stop/1]).

%% Types
-export_type([schema/0]).
-export_type([version/0]).
-export_type([ref/0]).
-export_type([snapshot/0]).
-export_type([history/0]).
-export_type([commit/0]).
-export_type([operation/0]).
-export_type([key/0]).
-export_type([value/0]).
-export_type([data/0]).

-type schema() :: integer().
-type version() :: integer().
-type ref() :: head | version().
-type snapshot() :: {version(), schema(), data()}.
-type history() :: {snapshot(), #{version() => commit()}}.
-type commit() :: {schema(), [operation()]}.
-type operation() :: {insert, {key(), value()}} | {update, {key(), value()}} | {delete, {key()}}.
-type key() :: term().
-type value() :: term().
-type data() :: #{key() => value()}.

%% API
-spec checkout(key()) -> value().
checkout(Key) ->
    checkout(head, Key).

-spec checkout(ref(), key()) -> value().
checkout(Ref, Key) ->
    dmt_server:checkout(Ref, Key).

-spec commit(commit()) -> version().
commit(Commit) ->
    dmt_server:commit(Commit).

-include_lib("dmt_proto/include/dmt_domain_thrift.hrl").

-spec current_schema() -> schema().
current_schema() ->
    ?REVISION.

-spec current_version() -> version().
current_version() ->
    {Version, _Schema, _Data} = dmt_server:get_snapshot(head),
    Version.

%% Supervisor callbacks

-spec init([]) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.

init([]) ->
    {ok, {
        #{strategy => rest_for_one, intensity => 10, period => 60},
        [
            % TODO
            % #{id => storage, start => {dmt_storage, start_link, []}, restart => permanent},
            #{id => dmt_server, start => {dmt_server, start_link, []}, restart => permanent}
        ]
    }}.

%% Application callbacks

-spec start(normal, any()) -> {ok, pid()} | {error, any()}.

start(_StartType, _StartArgs) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec stop(any()) -> ok.

stop(_State) ->
    ok.
