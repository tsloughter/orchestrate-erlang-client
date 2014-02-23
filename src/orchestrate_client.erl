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

%% TODO escape all parameters for URI-encoding
%% TODO remove code duplication when building common URIs
%% TODO do better error handling
%% TODO add an eunit test suite

%% API

delete(Collection) when is_bitstring(Collection); is_list(Collection) ->
    request(delete, Collection, [], []).

search(Collection, LuceneQuery) when is_bitstring(Collection); is_list(Collection) ->
    UriFragment = Collection ++ "?query=" ++ LuceneQuery,
    request(get, UriFragment, [], []).

search(Collection, LuceneQuery, Offset) when 
        is_bitstring(Collection); is_list(Collection),
        is_bitstring(LuceneQuery); is_list(LuceneQuery),
        is_integer(Offset), Offset >= 0
        ->
    UriFragment = Collection ++ "?query=" ++ LuceneQuery ++ "&offset=" ++ Offset,
    request(get, UriFragment, [], []).

search(Collection, LuceneQuery, Offset, Limit) when
        is_bitstring(Collection); is_list(Collection),
        is_bitstring(LuceneQuery); is_list(LuceneQuery),
        is_integer(Offset); Offset >= 0,
        is_integer(Limit); Limit >= 0
        ->
    UriFragment = Collection ++ "?query=" ++ LuceneQuery ++ "&offset=" ++ Offset ++ "&limit=" ++ Limit,
    request(get, UriFragment, [], []).

kv_get(Collection, Key) when is_bitstring(Collection); is_list(Collection), is_bitstring(Key); is_list(Key) ->
    UriFragment = Collection ++ "/" ++ Key,
    request(get, UriFragment, [], []).

kv_get(Collection, Key, Ref) ->
    UriFragment = Collection ++ "/" ++ Key ++ "/refs/" ++ Ref,
    request(get, UriFragment, [], []).

kv_list(Collection) when is_bitstring(Collection); is_list(Collection) ->
    request(get, Collection, [], []).

kv_list(Collection, Limit) when is_integer(Limit) ->
    UriFragment = Collection ++ "?limit=" ++ Limit,
    request(get, UriFragment, [], []);
kv_list(Collection, StartKey) ->
    UriFragment = Collection ++ "?startKey=" ++ StartKey,
    request(get, UriFragment, [], []).

kv_list(Collection, StartKey, Limit) when is_integer(Limit) ->
    UriFragment = Collection ++ "?startKey=" ++ StartKey ++ "&limit=" ++ Limit,
    request(get, UriFragment, [], []);
kv_list(Collection, StartKey, Inclusive) when Inclusive =:= true ->
    UriFragment = Collection ++ "?afterKey=" ++ StartKey,
    request(get, UriFragment, [], []).

kv_list(Collection, StartKey, Limit, Inclusive) when Inclusive =:= true ->
    UriFragment = Collection ++ "?afterKey=" ++ StartKey ++ "&limit=" ++ Limit,
    request(get, UriFragment, [], []);
kv_list(Collection, StartKey, Limit, Inclusive) when Inclusive =:= false ->
    UriFragment = Collection ++ "?startKey=" ++ StartKey ++ "&limit" ++ Limit,
    request(get, UriFragment, [], []).

kv_put(Collection, Key, Body) ->
    UriFragment = Collection ++ "/" ++ Key,
    request(put, UriFragment, [], Body).

kv_put(Collection, Key, Body, Match) when is_boolean(Match) ->
    UriFragment = Collection ++ "/" ++ Key,
    Headers = [
        {"if-none-match", Match}
    ],
    request(put, UriFragment, Headers, Body);
kv_put(Collection, Key, Body, Match) when is_bitstring(Match); is_list(Match) ->
    UriFragment = Collection ++ "/" ++ Key,
    Headers = [
        {"if-match", Match}
    ],
    request(put, UriFragment, Headers, Body).

kv_delete(Collection, Key) ->
    UriFragment = Collection ++ "/" ++ Key,
    request(delete, UriFragment, [], []).

kv_delete(Collection, Key, IfMatch) when is_list(IfMatch) ->
    UriFragment = Collection ++ "/" ++ Key,
    Headers = [
        {"if-match", IfMatch}
    ],
    request(delete, UriFragment, Headers, []).

kv_purge(Collection, Key) ->
    UriFragment = Collection ++ "/" ++ Key ++ "?purge=true",
    request(delete, UriFragment, [], []).

kv_purge(Collection, Key, IfMatch) when is_list(IfMatch) ->
    UriFragment = Collection ++ "/" ++ Key ++ "?purge=true",
    Headers = [
        {"if-match", IfMatch}
    ],
    request(delete, UriFragment, Headers, []).

events_get(Collection, Key, EventType) ->
    % TODO add some guard clauses
    UriFragment = Collection ++ "/" ++ Key ++ "/events/" ++ EventType,
    request(get, UriFragment, [], []).

events_get(Collection, Key, EventType, Start) ->
    % TODO add some guard clauses
    UriFragment = Collection ++ "/" ++ Key ++ "/events/" ++ EventType ++ "?start=" ++ Start,
    request(get, UriFragment, [], []).

events_get(Collection, Key, EventType, Start, End) ->
    % TODO add some guard clauses
    UriFragment = Collection ++ "/" ++ Key ++ "/events/" ++ EventType ++ "?start=" ++ Start ++ "&end=" ++ End,
    request(get, UriFragment, [], []).

events_put(Collection, Key, EventType) ->
    % TODO add some guard clauses
    UriFragment = Collection ++ "/" ++ Key ++ "/events/" ++ EventType,
    request(put, UriFragment, [], []).

events_put(Collection, Key, EventType, Timestamp) ->
    % TODO add some guard clauses
    UriFragment = Collection ++ "/" ++ Key ++ "/events/" ++ EventType ++ "?timestamp=" ++ Timestamp,
    request(put, UriFragment, [], []).

relation_get(Collection, Key, Kinds) ->
    % TODO add some guard clauses
    % TODO implement this
    {error, "not yet implemented."}.

relation_put(Collection, Key, Kind, ToCollection, ToKey) ->
    % TODO add some guard clauses
    UriFragment = Collection ++ "/" ++ Key ++ "/relation/" ++ Kind ++ "/" ++ ToCollection ++ "/" ++ ToKey,
    request(put, UriFragment, [], []).

relation_purge(Collection, Key, Kind, ToCollection, ToKey) ->
    % TODO add some guard clauses
    UriFragment = Collection ++ "/" ++ Key ++ "/relation/" ++ Kind ++ "/" ++ ToCollection ++ "/" ++ ToKey ++ "?purge=true",
    request(delete, UriFragment, [], []).

%% Internal API

request(Method, UriFragment, Headers, Body) ->
    Url = "https://api.orchestrate.io:443/v0/" ++ UriFragment,
    % TODO move API key to a "client" record
    % TODO expose some of the HttpOptions
    AuthHeaderValue = "Basic " ++ base64:encode_to_string("4390ea15-947e-4948-9984-9ba2c1d508a5:"),
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
