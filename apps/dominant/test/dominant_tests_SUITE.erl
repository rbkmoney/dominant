-module(dominant_tests_SUITE).
-include_lib("common_test/include/ct.hrl").
-compile(export_all).

%%
%% tests descriptions
%%
all() ->
    [
        dummy_test
    ].

%%
%% starting/stopping
%%
init_per_suite(C) ->
    {ok, Apps} = application:ensure_all_started(dominant),
    [{apps, Apps}|C].

end_per_suite(C) ->
    [application_stop(App) || App <- proplists:get_value(apps, C)].

application_stop(App=sasl) ->
    %% hack for preventing sasl deadlock
    %% http://erlang.org/pipermail/erlang-questions/2014-May/079012.html
    error_logger:delete_report_handler(cth_log_redirect),
    application:stop(App),
    error_logger:add_report_handler(cth_log_redirect),
    ok;
application_stop(App) ->
    application:stop(App).

%%
%% tests
%%
dummy_test(_C) ->
    ok.
