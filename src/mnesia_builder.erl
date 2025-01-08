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
                            case deploy_mb_ssg() of 
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




%-------------------------------------------------------------
%   
%-------------------------------------------------------------
-spec deploy_mb_ssg() -> ok | mb_error().
%-------------------------------------------------------------
deploy_mb_ssg() ->
    
    case (mb_utilities:file_exists(atom_to_list(?MB_SSG_TABLE) ++ ".erl") andalso mb_utilities:file_exists(atom_to_list(?MB_SSG_TABLE) ++ ".beam")) of 
        false -> 
            Result =
                mb_utilities:chain_execution([
                    fun() -> mb_ssg:new(mnesia_builder_ssg, "AllThingsErlang", "haitham.bouzeineddine@gmail.com", "MnesiaBuilder internal database") end,
                    fun(SSG) -> mb_ssg:add_schema(?MB_SSG_TABLE, SSG) end,
                    fun(SSG) -> mb_ssg:set_schema_type(bag, ?MB_SSG_TABLE, SSG) end,
                    fun(SSG) -> mb_ssg:add_schema_disc_copies([node()] ++ nodes(), ?MB_SSG_TABLE, SSG) end,

                    fun(SSG) -> mb_ssg:add_field(name, ?MB_SSG_TABLE, SSG) end,
                    fun(SSG) -> mb_ssg:set_field_type(atom, name, ?MB_SSG_TABLE, SSG) end,

                    %fun(SSG) -> mb_ssg:add_field(state, ?MB_SSG_TABLE, SSG) end,
                    %fun(SSG) -> mb_ssg:set_field_type(atom, state, ?MB_SSG_TABLE, SSG) end,

                    fun(SSG) -> mb_ssg:add_field(ssg, ?MB_SSG_TABLE, SSG) end,
                    fun(SSG) -> mb_ssg:set_field_type(map, ssg, ?MB_SSG_TABLE, SSG) end,

                    fun(SSG) -> mb_ssg:add_field(worker_pid, ?MB_SSG_TABLE, SSG) end,
                    fun(SSG) -> mb_ssg:set_field_type(term, worker_pid, ?MB_SSG_TABLE, SSG) end,

                    fun(SSG) -> mb_ssg:generate(?MB_SSG_TABLE, SSG) end,
                    fun(_) -> mb_utilities:compile_and_load(?MB_SSG_TABLE, atom_to_list(?MB_SSG_TABLE) ++ ".erl", "./") end
                ]),

                case Result of
                    {ok, _FinalResult} ->
                        io:format("[mb::mnesia_builder]: module ~p ready, checking table deployment~n", [?MB_SSG_TABLE]),
                     
                        case mb_utilities:table_exists(?MB_SSG_TABLE) of 
                            true -> 
                                io:format("[mb::mnesia_builder]: table ~p already deployed~n", [?MB_SSG_TABLE]),
                                ok;
                            false ->
                                io:format("[mb::mnesia_builder]: deploying table ~p~n", [?MB_SSG_TABLE]),
                                Module = ?MB_SSG_TABLE,
                                Module:deploy()
                        end;
                    
                    {error, Reason} ->
                        io:format("[mb::mnesia_builder]: deployment failed ~p~n", [Reason]),
                        {error, Reason}
                end;

            true -> ok 
    end.
