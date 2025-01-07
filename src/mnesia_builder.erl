-module(mnesia_builder).
-behaviour(application).
-include("../include/mb.hrl").

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    io:format("[mb::mnesia_builder]:start(...)~n"),

    MnesiaDir = mb_db_management:get_mnesia_dir(),

    case filelib:ensure_path(MnesiaDir) of 
        ok -> 
            application:set_env(mnesia, dir, MnesiaDir),
            case mb_utilities:start_mnesia() of
                ok ->
                    case mb_tables:deploy_schema() of 
                        ok -> 
                            case mb_supervisor:start_link() of
                                {ok, SupervisorPid} -> {ok, SupervisorPid}; % Set initial state as a tuple 
                                {error, Reason} -> {error, {application_start_failed, Reason}}  % terminate supervisor? 
                            end; 
                        {error, Reason} -> {error, {application_start_failed, Reason}}
                    end;
                {error, Reason} -> {error, {application_start_failed, Reason}}
            end;
        {error, Reason} -> {error, {application_start_failed, Reason}}
    end.


stop(_State) -> 
    io:format("[mb::mnesia_builder]:stop(...)~n"),
    ok.


