%%% @doc Public API, supervisor and application startup.
%%% @end

-module(dmt).
-behaviour(supervisor).
-behaviour(application).

%% API
-export([checkout/1]).
-export([checkout_object/2]).
-export([pull/1]).
-export([commit/2]).

%% Supervisor callbacks
-export([init/1]).

%% Application callbacks
-export([start/2]).
-export([stop/1]).

%% Type shortcuts
-export_type([version/0]).
-export_type([head/0]).
-export_type([ref/0]).
-export_type([snapshot/0]).
-export_type([commit/0]).
-export_type([operation/0]).
-export_type([history/0]).
-export_type([object_ref/0]).
-export_type([domain/0]).
-export_type([domain_object/0]).

-type version() :: dmt_domain_config_thrift:'Version'().
-type head() :: dmt_domain_config_thrift:'Head'().
-type ref() :: dmt_domain_config_thrift:'Reference'().
-type snapshot() :: dmt_domain_config_thrift:'Snapshot'().
-type commit() :: dmt_domain_config_thrift:'Commit'().
-type operation() :: dmt_domain_config_thrift:'Operation'().
-type history() :: dmt_domain_config_thrift:'History'().
-type object_ref() :: dmt_domain_thrift:'Reference'().
-type domain() :: dmt_domain_thrift:'Domain'().
-type domain_object() :: dmt_domain_thrift:'DomainObject'().

-include_lib("dmt_proto/include/dmt_domain_config_thrift.hrl").

%% API

-spec checkout(ref()) -> snapshot().
checkout(Reference) ->
    dmt_server:checkout(Reference).

-spec checkout_object(ref(), object_ref()) ->
    dmt_domain_config_thrift:'VersionedObject'().
checkout_object(Reference, ObjectReference) ->
    #'Snapshot'{version = Version, domain = Domain} = checkout(Reference),
    Object = dmt_domain:get_object(ObjectReference, Domain),
    #'VersionedObject'{version = Version, object = Object}.

-spec pull(version()) -> history().
pull(Version) ->
    case dmt_server:checkout({head, #'Head'{}}) of
        #'Snapshot'{version = Head} when Head > Version ->
            maps:from_list([{V, dmt_storage:get_commit(V)} || V <- lists:seq(Version + 1, Head)]);
        #'Snapshot'{} ->
            #{}
    end.

-spec commit(version(), commit()) -> version().
commit(Version, Commit) ->
    dmt_server:commit(Version, Commit).

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
