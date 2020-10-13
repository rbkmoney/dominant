-module(dmt_api_woody_utils).

-export([ensure_woody_deadline_set/2]).

%% API

-spec ensure_woody_deadline_set(woody_context:ctx(), woody_deadline:deadline()) -> woody_context:ctx().
ensure_woody_deadline_set(WoodyContext, Default) ->
    case woody_context:get_deadline(WoodyContext) of
        undefined ->
            woody_context:set_deadline(Default, WoodyContext);
        _Other ->
            WoodyContext
    end.
