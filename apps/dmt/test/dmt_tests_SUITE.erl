-module(dmt_tests_SUITE).
-include_lib("common_test/include/ct.hrl").

-export([all/0]).
-export([groups/0]).
-export([init_per_suite/1]).
-export([end_per_suite/1]).
-export([application_stop/1]).
-export([insert/1]).
-export([update/1]).
-export([delete/1]).

%%
%% tests descriptions
%%
-spec all() -> [term()].
all() ->
    [
        {group, basic_lifecycle}
    ].

-spec groups() -> [term()].
groups() ->
    [
        {basic_lifecycle, [parallel], [
            insert,
            update,
            delete
        ]}
    ].

%%
%% starting/stopping
-spec init_per_suite(term()) -> term().
init_per_suite(C) ->
    ok = application:set_env(dmt, storage, dmt_storage_env),
    {ok, Apps} = application:ensure_all_started(dmt),
    [{apps, Apps}|C].

-spec end_per_suite(term()) -> term().
end_per_suite(C) ->
    [application_stop(App) || App <- proplists:get_value(apps, C)].

-spec application_stop(term()) -> term().
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
-spec insert(term()) -> term().
insert(_C) ->
    Key = crypto:rand_bytes(10),
    Value = crypto:rand_bytes(10),
    not_found = (catch dmt:checkout(Key)),
    Version1 = dmt:current_version(),
    Version2 = dmt:commit({dmt:current_schema(), [{insert, Key, Value}]}),
    Version2 = dmt:current_version(),
    Value = dmt:checkout(Key),
    not_found = (catch dmt:checkout(Version1, Key)),
    Value = dmt:checkout(Version2, Key).

-spec update(term()) -> term().
update(_C) ->
    Key = crypto:rand_bytes(10),
    Value1 = crypto:rand_bytes(10),
    Value2 = crypto:rand_bytes(10),
    Version1 = dmt:commit({dmt:current_schema(), [{insert, Key, Value1}]}),
    Version2 = dmt:commit({dmt:current_schema(), [{update, Key, Value2}]}),
    Value1 = dmt:checkout(Version1, Key),
    Value2 = dmt:checkout(Version2, Key),
    Value2 = dmt:checkout(Key).

-spec delete(term()) -> term().
delete(_C) ->
    Key = crypto:rand_bytes(10),
    Value = crypto:rand_bytes(10),
    Version1 = dmt:commit({dmt:current_schema(), [{insert, Key, Value}]}),
    Version2 = dmt:commit({dmt:current_schema(), [{delete, Key}]}),
    Value = dmt:checkout(Version1, Key),
    not_found = (catch dmt:checkout(Version2, Key)),
    not_found = (catch dmt:checkout(Key)).
