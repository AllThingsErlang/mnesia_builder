-module(mb_supervisor).
-behaviour(supervisor).

-export([start_link/0, init/1, terminate/2]).

start_link() ->
    supervisor:start_link({local, mb_supervisor}, ?MODULE, []).

init([]) ->
    SupFlags = #{
        strategy => one_for_one, 
        intensity => 1, 
        period => 5
    },
    Processes = [
        {mb_server, {mb_server, start_link, []},
         permanent, 5000, worker, [mb_server]}
    ],
    {ok, {SupFlags, Processes}}.


terminate(Reason, _State) -> 
    io:format("[mb::supervisor::~p]: terminated, reason: ~p.~n", [self(), Reason]),
    ok.