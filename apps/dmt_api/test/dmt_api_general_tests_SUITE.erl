-module(dmt_api_general_tests_SUITE).
-include_lib("common_test/include/ct.hrl").

%% testspecs
-export([all/0]).
-export([init_per_suite/1]).
-export([end_per_suite/1]).

%% tests
-export([head_test/1]).

%% event handling
-behaviour(woody_event_handler).
-export([handle_event/2]).

%% tests descriptions

all() ->
    [
        head_test
    ].

%% starting/stopping

init_per_suite(C) ->
    {Apps, Opts} = application_start(dmt_api),
    [{applications, Apps}, {dmt_api_opts, Opts} | C].

end_per_suite(C) ->
    [application_stop(App) || App <- proplists:get_value(applications, C)].

application_start(App = dmt_api) ->
    Opts = [{host, "localhost"}, {port, 31999}],
    {genlib_app:start_application_with(App, Opts), Opts}.

application_stop(App = sasl) ->
    %% hack for preventing sasl deadlock
    %% http://erlang.org/pipermail/erlang-questions/2014-May/079012.html
    error_logger:delete_report_handler(cth_log_redirect),
    application:stop(App),
    error_logger:add_report_handler(cth_log_redirect),
    ok;
application_stop(App) ->
    application:stop(App).

%% tests

-include_lib("dmt_proto/include/dmt_domain_config_thrift.hrl").

head_test(C) ->
    Opts = ?config(dmt_api_opts, C),
    TestClient = construct_test_client(configurator, Opts),
    {ok, #'Version'{data = {rev, _}}} = call(TestClient, head, []).

%%

construct_test_client(Name, Opts) ->
    Host = ?config(host, Opts),
    Port = ?config(port, Opts),
    ReqID = genlib:to_list(Name) ++ erlang:ref_to_list(make_ref()),
    {Path, {Service, _, _}} = dmt_api:get_handler_spec(Name),
    Url = "http://" ++ Host ++ ":" ++ genlib:to_list(Port) ++ Path,
    Client = woody_client:new(ReqID, ?MODULE),
    {Url, Service, Client}.

call({Url, Service, Client} = _TestClient, Function, Args) ->
    {Result, _ClientNext} = woody_client:call(Client, {Service, Function, Args}, #{url => Url}),
    Result.

%%

-type woody_event_type() :: atom(). % FIXME
-type woody_event_meta() :: map().  % FIXME

-spec handle_event(woody_event_type(), woody_event_meta()) -> _.

handle_event(EventType, EventMeta) ->
    ct:log("[client] ~s ~p", [EventType, EventMeta]).
