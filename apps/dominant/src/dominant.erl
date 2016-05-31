%%% @doc Public API, supervisor and application startup.
%%% @end

-module(dominant).
-behaviour(supervisor).
-behaviour(application).

%% API
-export([head/0]).
-export([checkout/1]).
-export([checkout/2]).
-export([commit/2]).

%% Supervisor callbacks
-export([init/1]).

%% Application callbacks
-export([start/2]).
-export([stop/1]).

%% API

-type revision() :: pos_integer().
-type ref() :: head.

-type version_with(T) :: {Schema :: revision(), T}.
-type version() :: version_with(revision()).
-type version_ref() :: version_with(revision() | ref()).

-type object_ref() :: _.
-type object() :: _.
-type data() :: #{object_ref() => object()}.

-type operation() ::
    {insert, object()} |
    {update, object()} |
    {delete, object_ref()}.

-export_type([revision/0]).
-export_type([ref/0]).
-export_type([version/0]).
-export_type([version_ref/0]).
-export_type([object_ref/0]).
-export_type([object/0]).
-export_type([data/0]).

-spec head() ->
    {ok, version()} | {error, version_not_found}.

head() ->
    dmt_utils:map_ok(dominant_sync_server:head(), fun annotate_revision/1).

-spec checkout(version_ref()) ->
    {ok, {version(), data()}} | {error, version_not_found}.

checkout({SchemaRev, DataRef}) ->
    _ = validate_schema_revision(SchemaRev),
    dmt_utils:map_ok(dominant_sync_server:checkout(DataRef), fun annotate_revision/1).

-spec checkout(version_ref(), object_ref()) ->
    {ok, {version(), object()}} | {error, version_not_found | object_not_found}.

checkout(VersionRef, ObjectRef) ->
    case checkout(VersionRef) of
        {ok, {Version, Data}} ->
            case dmt_data:get_object(ObjectRef, Data) of
                {ok, Object} ->
                    {ok, {Version, Object}};
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

-spec commit(version(), operation()) -> {ok, version()} | {error, version_not_found}.

commit(_, _) ->
    error(noimpl).

%%

-include_lib("dmt_proto/include/dmt_domain_thrift.hrl").

get_schema_revision() ->
    ?REVISION.

annotate_revision({Rev, Data}) when is_integer(Rev) ->
    {{get_schema_revision(), Rev}, Data};
annotate_revision(Rev) when is_integer(Rev) ->
    {get_schema_revision(), Rev}.

validate_schema_revision(Rev) when Rev + 2 > ?REVISION ->
    ok;
validate_schema_revision(_) ->
    {error, incompatible_schema}.

%% Supervisor callbacks

-spec init([]) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.

init([]) ->
    {ok, {
        #{strategy => rest_for_one, intensity => 10, period => 60},
        [
            % TODO
            % #{id => storage, start => {dmt_storage, start_link, []}, restart => permanent},
            #{id => sync_server, start => {dmt_sync_server, start_link, []}, restart => permanent}
        ]
    }}.

%% Application callbacks

-spec start(normal, any()) -> {ok, pid()} | {error, any()}.

start(_StartType, _StartArgs) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec stop(any()) -> ok.

stop(_State) ->
    ok.
