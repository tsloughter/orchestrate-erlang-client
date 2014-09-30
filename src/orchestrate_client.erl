%% ===================================================================
%%   Copyright 2014 Orchestrate, Inc.
%%
%%   Licensed under the Apache License, Version 2.0 (the "License");
%%   you may not use this file except in compliance with the License.
%%   You may obtain a copy of the License at
%%
%%       http://www.apache.org/licenses/LICENSE-2.0
%%
%%   Unless required by applicable law or agreed to in writing, software
%%   distributed under the License is distributed on an "AS IS" BASIS,
%%   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%   See the License for the specific language governing permissions and
%%   limitations under the License.
%% ===================================================================

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

delete(Collection) when is_binary(Collection) ->
    delete(binary_to_list(Collection));
delete(Collection) ->
    request(delete, Collection).

search(Collection, LuceneQuery) when is_binary(Collection) ->
    search(binary_to_list(Collection), LuceneQuery);
search(Collection, LuceneQuery) when is_binary(LuceneQuery) ->
    search(Collection, binary_to_list(LuceneQuery));
search(Collection, LuceneQuery) ->
    UriFragment = search_uri_encode([Collection], LuceneQuery),
    request(get, UriFragment).

search(Collection, LuceneQuery, Offset) when is_binary(Collection) ->
    search(binary_to_list(Collection), LuceneQuery, Offset);
search(Collection, LuceneQuery, Offset) when is_binary(LuceneQuery) ->
    search(Collection, binary_to_list(LuceneQuery), Offset);
search(Collection, LuceneQuery, Offset) when is_integer(Offset) ->
    UriFragment = [search_uri_encode([Collection], LuceneQuery),
                  "&offset=", integer_to_list(Offset)],
    request(get, UriFragment).

search(Collection, LuceneQuery, Offset, Limit) when is_binary(Collection) ->
    search(binary_to_list(Collection), LuceneQuery, Offset, Limit);
search(Collection, LuceneQuery, Offset, Limit) when is_binary(LuceneQuery) ->
    search(Collection, binary_to_list(LuceneQuery), Offset, Limit);
search(Collection, LuceneQuery, Offset, Limit) when is_integer(Offset)
                                                  , is_integer(Limit) ->
    UriFragment = [search_uri_encode([Collection], LuceneQuery),
                  "&offset=", integer_to_list(Offset), "&limit=", integer_to_list(Limit)],
    request(get, UriFragment).

kv_get(Collection, Key) when is_binary(Collection) ->
    kv_get(binary_to_list(Collection), Key);
kv_get(Collection, Key) when is_binary(Key) ->
    kv_get(Collection, binary_to_list(Key));
kv_get(Collection, Key) ->
    UriFragment = uri_path_encode([Collection, Key]),
    request(get, UriFragment).

kv_get(Collection, Key, Ref) when is_binary(Collection) ->
    kv_get(binary_to_list(Collection), Key, Ref);
kv_get(Collection, Key, Ref) when is_binary(Key) ->
    kv_get(Collection, binary_to_list(Key), Ref);
kv_get(Collection, Key, Ref) when is_binary(Ref) ->
    kv_get(Collection, Key, binary_to_list(Ref));
kv_get(Collection, Key, Ref) ->
    UriFragment = [uri_path_encode([Collection, Key]), "/refs/", Ref],
    request(get, UriFragment).

kv_list(Collection) when is_binary(Collection) ->
    kv_list(binary_to_list(Collection));
kv_list(Collection) ->
    UriFragment = uri_path_encode([Collection]),
    request(get, UriFragment).

kv_list(Collection, Limit) when is_binary(Collection) ->
    kv_list(binary_to_list(Collection), Limit);
kv_list(Collection, Limit) when is_integer(Limit) ->
    UriFragment = [uri_path_encode([Collection]), "?limit=", integer_to_list(Limit)],
    request(get, UriFragment);
kv_list(Collection, StartKey) when is_binary(StartKey) ->
    kv_list(Collection, binary_to_list(StartKey));
kv_list(Collection, StartKey) ->
    UriFragment = kv_list_uri_encode([Collection], "startKey", StartKey),
    request(get, UriFragment).

kv_list(Collection, StartKey, true) when is_binary(Collection) ->
    kv_list(binary_to_list(Collection), StartKey, true);
kv_list(Collection, StartKey, true) when is_binary(StartKey) ->
    kv_list(Collection, binary_to_list(StartKey), true);
kv_list(Collection, StartKey, true) ->
    UriFragment = kv_list_uri_encode([Collection], "afterKey", StartKey),
    request(get, UriFragment);
kv_list(Collection, StartKey, Limit) when is_binary(Collection) ->
    kv_list(binary_to_list(Collection), StartKey, Limit);
kv_list(Collection, StartKey, Limit) when is_binary(StartKey) ->
    kv_list(Collection, binary_to_list(StartKey), Limit);
kv_list(Collection, StartKey, Limit) when is_integer(Limit) ->
    UriFragment = [kv_list_uri_encode([Collection], "startKey", StartKey),
                  "&limit=", integer_to_list(Limit)],
    request(get, UriFragment).

kv_list(Collection, StartKey, Limit, true) when is_binary(Collection) ->
    kv_list(binary_to_list(Collection), StartKey, Limit, true);
kv_list(Collection, StartKey, Limit, true) when is_binary(StartKey) ->
    kv_list(Collection, binary_to_list(StartKey), Limit, true);
kv_list(Collection, StartKey, Limit, true) ->
    UriFragment = [kv_list_uri_encode([Collection], "afterKey", StartKey),
                  "&limit=", integer_to_list(Limit)],
    request(get, UriFragment);
kv_list(Collection, StartKey, Limit, _) when is_binary(Collection) ->
    kv_list(binary_to_list(Collection), StartKey, Limit, false);
kv_list(Collection, StartKey, Limit, _) when is_binary(StartKey) ->
    kv_list(Collection, binary_to_list(StartKey), Limit, false);
kv_list(Collection, StartKey, Limit, _) ->
    UriFragment = [kv_list_uri_encode([Collection], "startKey", StartKey),
                  "&limit", integer_to_list(Limit)],
    request(get, UriFragment).

kv_put(Collection, Key, Body) when is_binary(Collection) ->
    kv_put(binary_to_list(Collection), Key, Body);
kv_put(Collection, Key, Body) when is_binary(Key) ->
    kv_put(Collection, binary_to_list(Key), Body);
kv_put(Collection, Key, Body) ->
    UriFragment = uri_path_encode([Collection, Key]),
    request(put, UriFragment, [], Body).

kv_put(Collection, Key, Body, Match) when is_binary(Collection) ->
    kv_put(binary_to_list(Collection), Key, Body, Match);
kv_put(Collection, Key, Body, Match) when is_binary(Key) ->
    kv_put(Collection, binary_to_list(Key), Body, Match);
kv_put(Collection, Key, Body, Match) when is_binary(Match) ->
    kv_put(Collection, Key, Body, binary_to_list(Match));
kv_put(Collection, Key, Body, Match) ->
    UriFragment = uri_path_encode([Collection, Key]),
    Headers = case Match of
        true ->
            [{"if-none-match", Match}];
        _ ->
            [{"if-match", Match}]
    end,
    request(put, UriFragment, Headers, Body).

kv_delete(Collection, Key) when is_binary(Collection) ->
    kv_delete(binary_to_list(Collection), Key);
kv_delete(Collection, Key) when is_binary(Key) ->
    kv_delete(Collection, binary_to_list(Key));
kv_delete(Collection, Key) ->
    UriFragment = uri_path_encode([Collection, Key]),
    request(delete, UriFragment).

kv_delete(Collection, Key, IfMatch) when is_binary(Collection) ->
    kv_delete(binary_to_list(Collection), Key, IfMatch);
kv_delete(Collection, Key, IfMatch) when is_binary(Key) ->
    kv_delete(Collection, binary_to_list(Key), IfMatch);
kv_delete(Collection, Key, IfMatch) when is_binary(IfMatch) ->
    kv_delete(Collection, Key, binary_to_list(IfMatch));
kv_delete(Collection, Key, IfMatch) ->
    UriFragment = uri_path_encode([Collection, Key]),
    Headers = [
        {"if-match", IfMatch}
    ],
    request(delete, UriFragment, Headers).

kv_purge(Collection, Key) when is_binary(Collection) ->
    kv_purge(binary_to_list(Collection), Key);
kv_purge(Collection, Key) when is_binary(Key) ->
    kv_purge(Collection, binary_to_list(Key));
kv_purge(Collection, Key) ->
    UriFragment = [uri_path_encode([Collection, Key]), "?purge=true"],
    request(delete, UriFragment).

kv_purge(Collection, Key, IfMatch) when is_binary(Collection) ->
    kv_purge(binary_to_list(Collection), Key, IfMatch);
kv_purge(Collection, Key, IfMatch) when is_binary(Key) ->
    kv_purge(Collection, binary_to_list(Key), IfMatch);
kv_purge(Collection, Key, IfMatch) when is_binary(IfMatch) ->
    kv_purge(Collection, Key, binary_to_list(IfMatch));
kv_purge(Collection, Key, IfMatch) ->
    UriFragment = [uri_path_encode([Collection, Key]), "?purge=true"],
    Headers = [
        {"if-match", IfMatch}
    ],
    request(delete, UriFragment, Headers).

events_get(Collection, Key, EventType) when is_binary(Collection) ->
    events_get(binary_to_list(Collection), Key, EventType);
events_get(Collection, Key, EventType) when is_binary(Key) ->
    events_get(Collection, binary_to_list(Key), EventType);
events_get(Collection, Key, EventType) when is_binary(EventType) ->
    events_get(Collection, Key, binary_to_list(EventType));
events_get(Collection, Key, EventType) ->
    UriFragment = events_uri_encode([Collection, Key], EventType),
    request(get, UriFragment).

events_get(Collection, Key, EventType, Start) when is_binary(Collection) ->
    events_get(binary_to_list(Collection), Key, EventType, Start);
events_get(Collection, Key, EventType, Start) when is_binary(Key) ->
    events_get(Collection, binary_to_list(Key), EventType, Start);
events_get(Collection, Key, EventType, Start) when is_binary(EventType) ->
    events_get(Collection, Key, binary_to_list(EventType), Start);
events_get(Collection, Key, EventType, Start) ->
    UriFragment = [events_uri_encode([Collection, Key], EventType), "?start=", Start],
    request(get, UriFragment).

events_get(Collection, Key, EventType, Start, End) when is_binary(Collection) ->
    events_get(binary_to_list(Collection), Key, EventType, Start, End);
events_get(Collection, Key, EventType, Start, End) when is_binary(Key) ->
    events_get(Collection, binary_to_list(Key), EventType, Start, End);
events_get(Collection, Key, EventType, Start, End) when is_binary(EventType) ->
    events_get(Collection, Key, binary_to_list(EventType), Start, End);
events_get(Collection, Key, EventType, Start, End) ->
    UriFragment = [events_uri_encode([Collection, Key], EventType), "?start=", Start, "&end=", End],
    request(get, UriFragment).

events_put(Collection, Key, EventType) when is_binary(Collection) ->
    events_put(binary_to_list(Collection), Key, EventType);
events_put(Collection, Key, EventType) when is_binary(Key) ->
    events_put(Collection, binary_to_list(Key), EventType);
events_put(Collection, Key, EventType) when is_binary(EventType) ->
    events_put(Collection, Key, binary_to_list(EventType));
events_put(Collection, Key, EventType) ->
    UriFragment = events_uri_encode([Collection, Key], EventType),
    request(put, UriFragment).

events_put(Collection, Key, EventType, Timestamp) when is_binary(Collection) ->
    events_put(binary_to_list(Collection), Key, EventType, Timestamp);
events_put(Collection, Key, EventType, Timestamp) when is_binary(Key) ->
    events_put(Collection, binary_to_list(Key), EventType, Timestamp);
events_put(Collection, Key, EventType, Timestamp) when is_binary(EventType) ->
    events_put(Collection, Key, binary_to_list(EventType), Timestamp);
events_put(Collection, Key, EventType, Timestamp) ->
    UriFragment = [events_uri_encode([Collection, Key], EventType), "?timestamp=", Timestamp],
    request(put, UriFragment).

relation_get(Collection, Key, Kinds) when is_binary(Collection) ->
    relation_get(binary_to_list(Collection), Key, Kinds);
relation_get(Collection, Key, Kinds) when is_binary(Key) ->
    relation_get(Collection, binary_to_list(Key), Kinds);
relation_get(Collection, Key, Kinds) ->
    UriFragment = uri_path_encode([Collection, Key, "relations", Kinds]),
    request(get, UriFragment).

relation_put(Collection, Key, Kind, ToCollection, ToKey) when is_binary(Collection) ->
    relation_put(binary_to_list(Collection), Key, Kind, ToCollection, ToKey);
relation_put(Collection, Key, Kind, ToCollection, ToKey) when is_binary(Key) ->
    relation_put(Collection, binary_to_list(Key), Kind, ToCollection, ToKey);
relation_put(Collection, Key, Kind, ToCollection, ToKey) when is_binary(Kind) ->
    relation_put(Collection, Key, binary_to_list(Kind), ToCollection, ToKey);
relation_put(Collection, Key, Kind, ToCollection, ToKey) when is_binary(ToCollection) ->
    relation_put(Collection, Key, Kind, binary_to_list(ToCollection), ToKey);
relation_put(Collection, Key, Kind, ToCollection, ToKey) when is_binary(ToKey) ->
    relation_put(Collection, Key, Kind, ToCollection, binary_to_list(ToKey));
relation_put(Collection, Key, Kind, ToCollection, ToKey) ->
    UriFragment = uri_path_encode([Collection, Key, "relation", Kind, ToCollection, ToKey]),
    request(put, UriFragment).

relation_purge(Collection, Key, Kind, ToCollection, ToKey) when is_binary(Collection) ->
    relation_purge(binary_to_list(Collection), Key, Kind, ToCollection, ToKey);
relation_purge(Collection, Key, Kind, ToCollection, ToKey) when is_binary(Key) ->
    relation_purge(Collection, binary_to_list(Key), Kind, ToCollection, ToKey);
relation_purge(Collection, Key, Kind, ToCollection, ToKey) when is_binary(Kind) ->
    relation_purge(Collection, Key, binary_to_list(Kind), ToCollection, ToKey);
relation_purge(Collection, Key, Kind, ToCollection, ToKey) when is_binary(ToCollection) ->
    relation_purge(Collection, Key, Kind, binary_to_list(ToCollection), ToKey);
relation_purge(Collection, Key, Kind, ToCollection, ToKey) when is_binary(ToKey) ->
    relation_purge(Collection, Key, Kind, ToCollection, binary_to_list(ToKey));
relation_purge(Collection, Key, Kind, ToCollection, ToKey) ->
    UriFragment = [uri_path_encode([Collection, Key, "relation", Kind, ToCollection, ToKey]), "?purge=true"],
    request(delete, UriFragment).

%% Internal API

kv_list_uri_encode(PathFragments, KeyTypeQueryParam, StartKey) ->
    [uri_path_encode(PathFragments), "?", KeyTypeQueryParam, "=", StartKey].

events_uri_encode(PathFragments, EventType) ->
    [uri_path_encode(PathFragments), "/events/", http_uri:encode(EventType)].

search_uri_encode(PathFragments, LuceneQuery) ->
    [uri_path_encode(PathFragments), "?query=", http_uri:encode(LuceneQuery)].

uri_path_encode(PathFragments) ->
    [ ["/", http_uri:encode(Path)] || Path <- PathFragments ].

request(Method, UriFragment) ->
    request(Method, UriFragment, [], []).

request(Method, UriFragment, Headers) ->
    request(Method, UriFragment, Headers, []).

request(Method, UriFragment, Headers, Body) ->
    Url = lists:flatten(["https://api.orchestrate.io:443/v0", UriFragment]),
    {ok, Auth} = application:get_env(orchestrate_client, http_auth),
    Headers2 = Headers ++ [
        {"accept", "application/json"},
        {"authorization", Auth},
        {"user-agent", "Orchestrate Erlang Client/master"}
    ],
    case Method of
        put ->
            Type = "application/json",
            Request = {Url, Headers2, Type, Body};
        _ ->
            Request = {Url, Headers2}
    end,
    %% TODO expose some of the HttpOptions
    HttpOptions = [
        {ssl, [{verify, verify_none}]},
        {timeout, 3000},
        {connect_timeout, 1000}
    ],
    Options = [{body_format, binary}],
    %% TODO don't set these options on every request
    httpc:set_options([
        {pipeline_timeout, 60000}
    ]),
    Response = httpc:request(Method, Request, HttpOptions, Options),
    case Response of
        {ok, {{_, Status, _}, Headers3, _Body2}} when Status == 201; Status == 204 ->
            {Status, Headers3, no_body};
        {ok, {{_, Status, _}, Headers3, Body2}} ->
            %% also decode Orchestrate error JSON responses
            {Status, Headers3, jsx:decode(Body2)};
        {error, _} ->
            Response
    end.
