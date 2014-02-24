-module(orchestrate_client).

-export([delete/1,
         search/2,
         search/3,
         search/4,
         kv_get/2,
         kv_get/3,
         kv_list/1,
         kv_list/2,
         kv_list/3,
         kv_list/4,
         kv_put/3,
         kv_put/4,
         kv_delete/2,
         kv_delete/3,
         kv_purge/2,
         kv_purge/3,
         events_get/3,
         events_get/4,
         events_get/5,
         events_put/3,
         events_put/4,
         relation_get/3,
         relation_put/5,
         relation_purge/5]).

%% TODO add specs to the public code
%% TODO escape all parameters for URI-encoding
%% TODO remove code duplication when building common URIs
%% TODO do better error handling
%% TODO add an eunit test suite

%% API

delete(Collection) ->
    request(delete, Collection, [], []).

search(Collection, LuceneQuery) ->
    UriFragment = [Collection, "?query=", LuceneQuery],
    request(get, UriFragment, [], []).

search(Collection, LuceneQuery, Offset) ->
    UriFragment = [Collection, "?query=", LuceneQuery, "&offset=", Offset],
    request(get, UriFragment, [], []).

search(Collection, LuceneQuery, Offset, Limit) ->
    UriFragment = [Collection, "?query=", LuceneQuery, "&offset=", Offset, "&limit=", Limit],
    request(get, UriFragment, [], []).

kv_get(Collection, Key) ->
    UriFragment = [Collection, "/", Key],
    request(get, UriFragment, [], []).

kv_get(Collection, Key, Ref) ->
    UriFragment = [Collection, "/", Key, "/refs/", Ref],
    request(get, UriFragment, [], []).

kv_list(Collection) ->
    request(get, Collection, [], []).

kv_list(Collection, Limit) when is_integer(Limit) ->
    UriFragment = [Collection, "?limit=", Limit],
    request(get, UriFragment, [], []);
kv_list(Collection, StartKey) ->
    UriFragment = [Collection, "?startKey=", StartKey],
    request(get, UriFragment, [], []).

kv_list(Collection, StartKey, true) ->
    UriFragment = [Collection, "?afterKey=", StartKey],
    request(get, UriFragment, [], []);
kv_list(Collection, StartKey, Limit) when is_integer(Limit) ->
    UriFragment = [Collection, "?startKey=", StartKey, "&limit=", Limit],
    request(get, UriFragment, [], []).

kv_list(Collection, StartKey, Limit, true) ->
    UriFragment = [Collection, "?afterKey=", StartKey, "&limit=", Limit],
    request(get, UriFragment, [], []);
kv_list(Collection, StartKey, Limit, _) ->
    UriFragment = [Collection, "?startKey=", StartKey, "&limit", Limit],
    request(get, UriFragment, [], []).

kv_put(Collection, Key, Body) ->
    UriFragment = [Collection, "/", Key],
    request(put, UriFragment, [], Body).

kv_put(Collection, Key, Body, Match) ->
    UriFragment = [Collection, "/", Key],
    Headers = case Match of
        true ->
            [{"if-none-match", Match}];
        _ ->
            [{"if-match", Match}]
    end,
    request(put, UriFragment, Headers, Body).

kv_delete(Collection, Key) ->
    UriFragment = [Collection, "/", Key],
    request(delete, UriFragment, [], []).

kv_delete(Collection, Key, IfMatch) ->
    UriFragment = [Collection, "/", Key],
    Headers = [
        {"if-match", IfMatch}
    ],
    request(delete, UriFragment, Headers, []).

kv_purge(Collection, Key) ->
    UriFragment = [Collection, "/", Key, "?purge=true"],
    request(delete, UriFragment, [], []).

kv_purge(Collection, Key, IfMatch) ->
    UriFragment = [Collection, "/", Key, "?purge=true"],
    Headers = [
        {"if-match", IfMatch}
    ],
    request(delete, UriFragment, Headers, []).

events_get(Collection, Key, EventType) ->
    UriFragment = [Collection, "/", Key, "/events/", EventType],
    request(get, UriFragment, [], []).

events_get(Collection, Key, EventType, Start) ->
    UriFragment = [Collection, "/", Key, "/events/", EventType, "?start=", Start],
    request(get, UriFragment, [], []).

events_get(Collection, Key, EventType, Start, End) ->
    UriFragment = [Collection, "/", Key, "/events/", EventType, "?start=", Start, "&end=", End],
    request(get, UriFragment, [], []).

events_put(Collection, Key, EventType) ->
    UriFragment = [Collection, "/", Key, "/events/", EventType],
    request(put, UriFragment, [], []).

events_put(Collection, Key, EventType, Timestamp) ->
    UriFragment = [Collection, "/", Key, "/events/", EventType, "?timestamp=", Timestamp],
    request(put, UriFragment, [], []).

relation_get(Collection, Key, Kinds) ->
    % TODO add some guard clauses
    % TODO implement this
    {error, "not yet implemented."}.

relation_put(Collection, Key, Kind, ToCollection, ToKey) ->
    UriFragment = [Collection, "/", Key, "/relation/", Kind, "/", ToCollection, "/", ToKey],
    request(put, UriFragment, [], []).

relation_purge(Collection, Key, Kind, ToCollection, ToKey) ->
    UriFragment = [Collection, "/", Key, "/relation/", Kind, "/", ToCollection, "/", ToKey, "?purge=true"],
    request(delete, UriFragment, [], []).

%% Internal API

request(Method, UriFragment, Headers, Body) ->
    Url = lists:flatten("https://api.orchestrate.io:443/v0/", UriFragment),
    % TODO move API key to a "client" record
    % TODO expose some of the HttpOptions
    AuthHeaderValue = lists:flatten("Basic ", base64:encode_to_string("4390ea15-947e-4948-9984-9ba2c1d508a5:")),
    Headers2 = [
        {"accept", "application/json"},
        {"authorization", AuthHeaderValue},
        {"user-agent", "Orchestrate Java Client/0.2.0"}
    ] ++ Headers,
    case Method of
        put ->
            Type = "application/json",
            Request = {Url, Headers2, Type, Body};
        _ ->
            Request = {Url, Headers2}
    end,
    HttpOptions = [
        {ssl, [{verify, verify_none}]},
        {timeout, 3000},
        {connect_timeout, 1000}
    ],
    Options = [],
    Response = httpc:request(Method, Request, HttpOptions, Options),
    case Response of
        {ok, {{_, Status, _}, Headers3, Body2}} when Status >= 200, Status < 400 ->
            {Headers3, jiffy:decode(Body2)};
        _ ->
            Response
    end.
