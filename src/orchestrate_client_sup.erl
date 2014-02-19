
-module(orchestrate_client_sup).

-behaviour(supervisor).

%% API exports
-export([start_link/0,
         init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% API

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% Supervisor

init([]) ->
    {ok, {{one_for_one, 10, 10}, []}}.
