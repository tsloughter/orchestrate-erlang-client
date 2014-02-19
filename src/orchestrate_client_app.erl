-module(orchestrate_client_app).

-behaviour(application).

-export([start/2,
         stop/1]).

%% API

start(_StartType, _StartArgs) ->
    orchestrate_client_sup:start_link().

stop(_State) ->
    ok.
