-module(mnesia_builder).
-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    io:format("mnesia_builder:start(...)~n"),

    case mb_supervisor:start_link() of
        {ok, SupervisorPid} -> 
            {ok, SupervisorPid}; % Set initial state as a tuple 
        {error, Reason} ->
            {error, {supervisor_start_failed, Reason}}
    end.


stop(_State) -> 
    io:format("mnesia_builder:stop(...)~n"),
    ok.