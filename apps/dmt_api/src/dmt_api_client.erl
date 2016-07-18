-module(dmt_api_client).

-export([commit/2]).
-export([checkout/1]).
-export([pull/1]).
-export([checkout_object/2]).


-spec commit(dmt:version(), dmt:commit()) -> dmt:version().
commit(Version, Commit) ->
    call(repository, 'Commit', [Version, Commit]).

-spec checkout(dmt:ref()) -> dmt:snapshot().
checkout(Reference) ->
    call(repository, 'Checkout', [Reference]).

-spec pull(dmt:version()) -> dmt:history().
pull(Version) ->
    call(repository, 'Pull', [Version]).

-spec checkout_object(dmt:ref(), dmt:object_ref()) -> dmt:domain_object().
checkout_object(Reference, ObjectReference) ->
    call(repository_client, 'checkoutObject', [Reference, ObjectReference]).


call(ServiceName, Function, Args) ->
    Host = application:get_env(dmt, client_host, "dominant"),
    Port = integer_to_list(application:get_env(dmt, client_port, 8800)),
    {Path, {Service, _Handler, _Opts}} = dmt_api:get_handler_spec(ServiceName),
    Call = {Service, Function, Args},
    Server = #{url => Host ++ ":" ++ Port ++ Path},
    Context = woody_client:new_context(woody_client:make_id(<<"dmt_client">>), dmt_api_event_handler),
    try woody_client:call(Context, Call, Server) of
        {ok, _Context} ->
            ok;
        {{ok, Response}, _Context} ->
            Response
    catch {{exception, Exception}, _Context} ->
        throw(Exception)
    end.
