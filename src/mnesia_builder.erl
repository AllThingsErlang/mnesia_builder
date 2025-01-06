-module(mnesia_builder).
-behaviour(application).
-include("../include/mb.hrl").

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    io:format("mnesia_builder:start(...)~n"),

    MnesiaDir = mb_db_management:get_mnesia_dir(),

    case filelib:ensure_path(MnesiaDir) of 
        ok -> 
            application:set_env(mnesia, dir, MnesiaDir),
            case mb_utilities:start_mnesia() of
                ok ->
                    case mb_supervisor:start_link() of
                        {ok, SupervisorPid} -> 
                            case deploy_schema() of 
                                ok -> 
                                    {ok, SupervisorPid}; % Set initial state as a tuple 

                                {error, Reason} -> {error, {application_start_failed, Reason}}  % terminate supervisor? 
                            end; 

                        {error, Reason} -> {error, {application_start_failed, Reason}}
                    end;
                {error, Reason} -> {error, {application_start_failed, Reason}}
            end;

        {error, Reason} -> {error, {application_start_failed, Reason}}
    end.


stop(_State) -> 
    io:format("mnesia_builder:stop(...)~n"),
    ok.


deploy_schema() -> 

    %{ok, S} = mb_api:connect(),

    %Result = ok,

    %Result = mb_api:new_ssg(S, mnesia_builder_ssg, "All Things Erlang", "haitham@gmail.com", "SSG used by mnesia_builder"),
    %Result = mb_api:add_schema(S, ssg_table),
    %Result = mb_api:set_schema_type(S, ssg_table, bag),
    %Result = mb_api:add_schema_disc_copies_local(S, ssg_table),

    ok.

