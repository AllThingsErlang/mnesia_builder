-module(mnesia_builder).
-behaviour(application).
-include("../include/mb.hrl").

-export([start/2, stop/1]).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
start(_StartType, _StartArgs) ->
    io:format("[mb::mnesia_builder::start]: (...)~n"),

    MnesiaDir = mb_db_management:get_mnesia_dir(),

    case filelib:ensure_path(MnesiaDir) of 
        ok -> 
            io:format("[mb::mnesia_builder::start]: mnesia dir ~s~n", [MnesiaDir]),

            application:set_env(mnesia, dir, MnesiaDir),
            case mb_utilities:start_mnesia() of
                ok ->
                    io:format("[mb::mnesia_builder::start]: mnesia started~n"),

                    case mb_db_management:setup_mnesia_schema() of 
                        ok ->    
                            io:format("[mb::mnesia_builder::start]: mnesia schema installed~n"),                
                            case mb_tables:deploy_mb_ssg() of 
                                ok -> 
                                    io:format("[mb::mnesia_builder::start]: mb ssg deployed~n"),
                                    case mb_supervisor:start_link() of
                                        {ok, SupervisorPid} -> 
                                            io:format("[mb::mnesia_builder::start]: start completed~n"),
                                            {ok, SupervisorPid}; % Set initial state as a tuple 
                                        {error, Reason} -> {error, {application_start_failed, Reason}}  
                                    end; 
                                {error, Reason} -> {error, {application_start_failed, Reason}}
                            end;
                        {error, Reason} -> {error, {application_start_failed, Reason}}
                    end;
                {error, Reason} -> {error, {application_start_failed, Reason}}
            end;
        {error, Reason} -> {error, {application_start_failed, Reason}}
    end.

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
stop(_State) -> 
    io:format("[mb::mnesia_builder::stop]: (...)~n"),
    ok.



