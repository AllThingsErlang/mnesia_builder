-module(mb_supervisor).
-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, mb_supervisor}, ?MODULE, []).

init([]) ->
    Processes = [
        {mb_server, {mb_server, start_link, []},
         permanent, 5000, worker, [mb_server]}
    ],
    {ok, {{one_for_one, 1, 5}, Processes}}.
