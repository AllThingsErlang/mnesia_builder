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
                    case deploy_schema() of 
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


deploy_schema() -> 

    SSG1 = mb_ssg:new(mnesia_builder_ssg, "AllThingsErlang", "haitham.bouzeineddine@gmail.com", "MnesiaBuilder internal database"),
    SSG2 = mb_ssg:add_schema(ssg_table, SSG1),
    SSG3 = mb_ssg:set_schema_type(bag, ssg_table, SSG2),
    SSG4 = mb_ssg:add_schema_disc_copies([node()] ++ nodes(), ssg_table, SSG3),
    SSG5 = mb_ssg:add_field(name_version, ssg_table, SSG4),
    SSG6 = mb_ssg:set_field_type(tuple, name_version, ssg_table, SSG5),
    SSG7 = mb_ssg:add_field(state, ssg_table, SSG6),
    SSG8 = mb_ssg:set_field_type(atom, state, ssg_table, SSG7),
    SSG9 = mb_ssg:add_field(ssg, ssg_table, SSG8),
    SSG10 = mb_ssg:set_field_type(map, ssg, ssg_table, SSG9),

    io:format("~n~n~p~n~n", [SSG10]),

    Result = mb_db_management:install(SSG10),

    io:format("[mb::mnesia_builder]: install result ~p~n", [Result]),

    ok.

