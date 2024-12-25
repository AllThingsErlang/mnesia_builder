-module(mnesia_builder).
-behaviour(application).

-export([start/2, stop/1]).

% Start the application
-spec start(normal, []) -> {ok, pid()} | {error, term()}.
start(_StartType, _StartArgs) ->
 % Ensure Mnesia and its dependencies are started
    io:format("mnesia_builder:start(...)~n"),
    
    case application:ensure_all_started(mnesia) of
        {ok, _} ->
            io:format("dependent application started successfully.~n"),
            % Start the supervisor
            mb_supervisor:start_link();

        {error, Reason} ->
            io:format("dependent application(s) not started: ~p~n", [Reason]),
            {error, Reason}
    end.

% Stop the application
-spec stop(State :: term()) -> ok.
stop(_State) ->
    ok.
