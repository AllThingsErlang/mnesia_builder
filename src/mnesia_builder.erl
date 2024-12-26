-module(mnesia_builder).
-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    io:format("mnesia_builder:start(...)~n"),

    case application:ensure_all_started(mnesia) of
        {ok, _} ->
            io:format("dependent application started successfully.~n"),
            case mb_supervisor:start_link() of
                {ok, SupervisorPid} -> 
                    {ok, {SupervisorPid, []}}; % Set initial state as a tuple 
                {error, Reason} ->
                    {error, {supervisor_start_failed, Reason}}
            end;

        {error, Reason} ->
            io:format("dependent application(s) not started: ~p~n", [Reason]),
            {error, Reason}
    end.

stop({SupervisorPid, _}) -> 
    supervisor:terminate_child(SupervisorPid, shutdown),
    ok;
stop(_) -> 
    ok.