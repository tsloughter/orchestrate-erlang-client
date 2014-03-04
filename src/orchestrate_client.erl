-module(orchestrate_client).

-export([set_apikey/1,
         delete/1,
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
%% TODO do better error handling
%% TODO add an eunit test suite

%% API

set_apikey(ApiKey) when is_list(ApiKey) ->
    set_apikey(list_to_binary(ApiKey));
set_apikey(ApiKey) when is_binary(ApiKey) ->
    EncodedToken = base64:encode(<<ApiKey/binary, ":">>),
    Auth = <<"Basic ", EncodedToken/binary>>,
    application:set_env(orchestrate_client, http_auth, binary_to_list(Auth)).

delete(Collection) ->
    request(delete, Collection, [], []).

search(Collection, LuceneQuery) ->
    UriFragment = search_uri_encode([Collection], LuceneQuery),
    request(get, UriFragment, [], []).

search(Collection, LuceneQuery, Offset) ->
    UriFragment = [search_uri_encode([Collection], LuceneQuery), "&offset=", Offset],
    request(get, UriFragment, [], []).

search(Collection, LuceneQuery, Offset, Limit) ->
    UriFragment = [search_uri_encode([Collection], LuceneQuery), "&offset=", Offset, "&limit=", Limit],
    request(get, UriFragment, [], []).

kv_get(Collection, Key) ->
    UriFragment = uri_path_encode([Collection, Key]),
    request(get, UriFragment, [], []).

kv_get(Collection, Key, Ref) ->
    UriFragment = [uri_path_encode([Collection, Key]), "/refs/", Ref],
    request(get, UriFragment, [], []).

kv_list(Collection) ->
    UriFragment = uri_path_encode([Collection]),
    request(get, UriFragment, [], []).

kv_list(Collection, Limit) when is_integer(Limit) ->
    UriFragment = [uri_path_encode([Collection]), "?limit=", Limit],
    request(get, UriFragment, [], []);
kv_list(Collection, StartKey) ->
    UriFragment = kv_list_uri_encode([Collection], "startKey", StartKey),
    request(get, UriFragment, [], []).

kv_list(Collection, StartKey, true) ->
    UriFragment = kv_list_uri_encode([Collection], "afterKey", StartKey),
    request(get, UriFragment, [], []);
kv_list(Collection, StartKey, Limit) when is_integer(Limit) ->
    UriFragment = [kv_list_uri_encode([Collection], "startKey", StartKey), "&limit=", Limit],
    request(get, UriFragment, [], []).

kv_list(Collection, StartKey, Limit, true) ->
    UriFragment = [kv_list_uri_encode([Collection], "afterKey", StartKey), "&limit=", Limit],
    request(get, UriFragment, [], []);
kv_list(Collection, StartKey, Limit, _) ->
    UriFragment = [kv_list_uri_encode([Collection], "startKey", StartKey), "&limit", Limit],
    request(get, UriFragment, [], []).

kv_put(Collection, Key, Body) ->
    UriFragment = uri_path_encode([Collection, Key]),
    request(put, UriFragment, [], Body).

kv_put(Collection, Key, Body, Match) ->
    UriFragment = uri_path_encode([Collection, Key]),
    Headers = case Match of
        true ->
            [{"if-none-match", Match}];
        _ ->
            [{"if-match", Match}]
    end,
    request(put, UriFragment, Headers, Body).

kv_delete(Collection, Key) ->
    UriFragment = uri_path_encode([Collection, Key]),
    request(delete, UriFragment, [], []).

kv_delete(Collection, Key, IfMatch) ->
    UriFragment = uri_path_encode([Collection, Key]),
    Headers = [
        {"if-match", IfMatch}
    ],
    request(delete, UriFragment, Headers, []).

kv_purge(Collection, Key) ->
    UriFragment = [uri_path_encode([Collection, Key]), "?purge=true"],
    request(delete, UriFragment, [], []).

kv_purge(Collection, Key, IfMatch) ->
    UriFragment = [uri_path_encode([Collection, Key]), "?purge=true"],
    Headers = [
        {"if-match", IfMatch}
    ],
    request(delete, UriFragment, Headers, []).

events_get(Collection, Key, EventType) ->
    UriFragment = events_uri_encode([Collection, Key], EventType),
    request(get, UriFragment, [], []).

events_get(Collection, Key, EventType, Start) ->
    UriFragment = [events_uri_encode([Collection, Key], EventType), "?start=", Start],
    request(get, UriFragment, [], []).

events_get(Collection, Key, EventType, Start, End) ->
    UriFragment = [events_uri_encode([Collection, Key], EventType), "?start=", Start, "&end=", End],
    request(get, UriFragment, [], []).

events_put(Collection, Key, EventType) ->
    UriFragment = events_uri_encode([Collection, Key], EventType),
    request(put, UriFragment, [], []).

events_put(Collection, Key, EventType, Timestamp) ->
    UriFragment = [events_uri_encode([Collection, Key], EventType), "?timestamp=", Timestamp],
    request(put, UriFragment, [], []).

relation_get(Collection, Key, Kinds) ->
    UriFragment = uri_path_encode([Collection, Key, "relations", Kinds]),
    request(get, UriFragment, [], []).

relation_put(Collection, Key, Kind, ToCollection, ToKey) ->
    UriFragment = uri_path_encode([Collection, Key, "relation", Kind, ToCollection, ToKey]),
    request(put, UriFragment, [], []).

relation_purge(Collection, Key, Kind, ToCollection, ToKey) ->
    UriFragment = [uri_path_encode([Collection, Key, "relation", Kind, ToCollection, ToKey]), "?purge=true"],
    request(delete, UriFragment, [], []).

%% Internal API

kv_list_uri_encode(PathFragments, KeyTypeQueryParam, StartKey) ->
    [uri_path_encode(PathFragments), "?", KeyTypeQueryParam, "=", StartKey].

events_uri_encode(PathFragments, EventType) ->
    [uri_path_encode(PathFragments), "/events/", http_uri:encode(EventType)].

search_uri_encode(PathFragments, LuceneQuery) ->
    [uri_path_encode(PathFragments), "?query=", http_uri:encode(LuceneQuery)].

uri_path_encode(PathFragments) ->
    [ ["/", http_uri:encode(Path)] || Path <- PathFragments ].

request(Method, UriFragment, Headers, Body) ->
    Url = lists:flatten(["https://api.orchestrate.io:443/v0", UriFragment]),
    {ok, Auth} = application:get_env(orchestrate_client, http_auth),
    % TODO expose some of the HttpOptions
    Headers2 = Headers ++ [
        {"accept", "application/json"},
        {"authorization", Auth},
        {"user-agent", "Orchestrate Java Client/0.2.0"}
    ],
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
    % TODO don't set these options on every request
    httpc:set_options([
        {pipeline_timeout, 60000}
    ]),
    Response = httpc:request(Method, Request, HttpOptions, Options),
    case Response of
        {ok, {{_, Status, _}, Headers3, _Body2}} when Status == 201; Status == 204 ->
            {Status, Headers3, no_body};
        {ok, {{_, Status, _}, Headers3, Body2}} ->
            % also decode Orchestrate error JSON responses
            {Status, Headers3, jiffy:decode(Body2)};
        {error, _} ->
            Response
    end.
