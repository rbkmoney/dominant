-module(dmt_history).

-export([head/1]).
-export([head/2]).
-export([travel/3]).

-include_lib("dmt_proto/include/dmt_domain_config_thrift.hrl").

-spec head(dmt:history()) -> dmt:snapshot().
head(History) when map_size(History) =:= 0 ->
    #'Snapshot'{version = 0, domain = dmt_domain:new()};
head(History) ->
    head(History, #'Snapshot'{version = 0, domain = dmt_domain:new()}).

-spec head(dmt:history(), dmt:snapshot()) -> dmt:snapshot().
head(History, Snapshot) ->
    Head = lists:max(maps:keys(History)),
    travel(Head, History, Snapshot).

-spec travel(dmt:version(), dmt:history(), dmt:snapshot()) -> dmt:snapshot().
travel(To, _History, #'Snapshot'{version = From} = Snapshot)
when To =:= From ->
    Snapshot;
travel(To, History, #'Snapshot'{version = From, domain = Domain})
when To > From ->
    #'Commit'{ops = Ops} = maps:get(From + 1, History),
    NextSnapshot = #'Snapshot'{
        version = From + 1,
        domain = dmt_domain:apply_operations(Ops, Domain)
    },
    travel(To, History, NextSnapshot);
travel(To, History, #'Snapshot'{version = From, domain = Domain})
when To < From ->
    #'Commit'{ops = Ops} = maps:get(From, History),
    PreviousSnapshot = #'Snapshot'{
        version = From - 1,
        domain = dmt_domain:revert_operations(Ops, Domain)
    },
    travel(To, History, PreviousSnapshot).
