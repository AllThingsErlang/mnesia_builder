-module(mnesia_builder_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, mnesia_builder_sup}, ?MODULE, []).

init([]) ->
    Processes = [
        {db_server, {db_server, start_link, []},
         permanent, 5000, worker, [db_server]}
    ],
    {ok, {{one_for_one, 1, 5}, Processes}}.
