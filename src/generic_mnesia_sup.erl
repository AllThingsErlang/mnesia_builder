-module(generic_mnesia_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, generic_mnesia_sup}, ?MODULE, []).

init([]) ->
    Processes = [
        % Start schema_modeller server
        {schema_modeller, {schema_modeller, start_link, []},
         permanent, 5000, worker, [schema_modeller]},
        
        % Start db_access server
        {db_access, {db_access, start_link, []},
         permanent, 5000, worker, [db_access]}
    ],
    {ok, {{one_for_one, 1, 5}, Processes}}.
