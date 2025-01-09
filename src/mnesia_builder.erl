-module(mnesia_builder).
-behaviour(application).
-include("../include/mb.hrl").

-export([start/2, stop/1]).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
start(_StartType, _StartArgs) ->
    io:format("[mb::mnesia_builder::start]: (...)~n"),

    case setup_directories() of 
        ok -> 
            io:format("[mb::mnesia_builder::start]: all dir setup ok ~n"),
            
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
    
    MnesiaBuilderSsgModuleSrcDir = filename:absname(?AUTO_GEN_MB_SRC_DIR),
    MnesiaBuilderSsgModuleIncludeDir = filename:absname(?AUTO_GEN_MB_INCLUDE_DIR),
    MnesiaBuilderSsgModuleBeamDir = filename:absname(?AUTO_GEN_MB_EBIN_DIR),
    
    MnesiaBuilderSsgModuleSrc = MnesiaBuilderSsgModuleSrcDir ++ "/" ++ atom_to_list(?MB_SSG_MODULE) ++ ".erl",
    %MnesiaBuilderSsgModuleBeam = MnesiaBuilderSsgModuleBeamDir ++ "/" ++ atom_to_list(?MB_SSG_MODULE) ++ ".beam",

    Result =
        mb_utilities:chain_execution([
            fun() -> mb_ssg:new(?MB_SSG_NAME, "AllThingsErlang", "haitham.bouzeineddine@gmail.com", "MnesiaBuilder internal database") end,
            fun(SSG) -> mb_ssg:add_schema(?INTERNAL_SSG_TABLE, SSG) end,
            fun(SSG) -> mb_ssg:set_schema_type(set, ?INTERNAL_SSG_TABLE, SSG) end,
            fun(SSG) -> mb_ssg:add_schema_disc_copies([node()] ++ nodes(), ?INTERNAL_SSG_TABLE, SSG) end,

            fun(SSG) -> mb_ssg:add_field(name, ?INTERNAL_SSG_TABLE, SSG) end,
            fun(SSG) -> mb_ssg:set_field_type(atom, name, ?INTERNAL_SSG_TABLE, SSG) end,

            %fun(SSG) -> mb_ssg:add_field(state, ?INTERNAL_SSG_TABLE, SSG) end,
            %fun(SSG) -> mb_ssg:set_field_type(atom, state, ?INTERNAL_SSG_TABLE, SSG) end,

            fun(SSG) -> mb_ssg:add_field(ssg, ?INTERNAL_SSG_TABLE, SSG) end,
            fun(SSG) -> mb_ssg:set_field_type(map, ssg, ?INTERNAL_SSG_TABLE, SSG) end,

            fun(SSG) -> mb_ssg:add_field(worker_pid, ?INTERNAL_SSG_TABLE, SSG) end,
            fun(SSG) -> mb_ssg:set_field_type(term, worker_pid, ?INTERNAL_SSG_TABLE, SSG) end,

            fun(SSG) -> mb_ssg:generate(?MB_SSG_MODULE, MnesiaBuilderSsgModuleSrcDir, MnesiaBuilderSsgModuleIncludeDir, SSG) end,
            fun() -> mb_utilities:compile_and_load(?MB_SSG_MODULE, MnesiaBuilderSsgModuleSrc, MnesiaBuilderSsgModuleBeamDir) end
        ]),

    case Result of
        {ok, _FinalResult} ->
            io:format("[mb::mnesia_builder]: module ~p ready, checking table deployment~n", [?INTERNAL_SSG_TABLE]),
            
            case mb_utilities:table_exists(?INTERNAL_SSG_TABLE) of 
                true -> 
                    io:format("[mb::mnesia_builder]: table ~p already deployed~n", [?INTERNAL_SSG_TABLE]),
                    ok;
                false ->
                    io:format("[mb::mnesia_builder]: deploying table ~p~n", [?INTERNAL_SSG_TABLE]),
                    Module = ?MB_SSG_MODULE,
                    Module:deploy()
            end;
        
        {error, Reason} ->
            io:format("[mb::mnesia_builder]: deployment failed ~p~n", [Reason]),
            {error, Reason}
    end.


%-------------------------------------------------------------
%   
%-------------------------------------------------------------
-spec setup_directories() -> ok | mb_error().
%-------------------------------------------------------------
setup_directories() -> 
    MnesiaDir = mb_db_management:get_mnesia_dir(),

    mb_utilities:chain_execution([
        fun() -> filelib:ensure_path(MnesiaDir) == ok end,
        fun() -> application:set_env(mnesia, dir, MnesiaDir) end, 

        fun() -> filelib:ensure_path(?AUTO_GEN_CLIENT_SRC_DIR) == ok end,
        fun() -> filelib:ensure_path(?AUTO_GEN_CLIENT_EBIN_DIR) == ok end,
        fun() -> filelib:ensure_path(?AUTO_GEN_CLIENT_INCLUDE_DIR) == ok end,
        fun() -> filelib:ensure_path(?AUTO_GEN_MB_SRC_DIR) == ok end,
        fun() -> filelib:ensure_path(?AUTO_GEN_MB_EBIN_DIR) == ok end,
        fun() -> filelib:ensure_path(?AUTO_GEN_MB_INCLUDE_DIR) == ok end,

        fun() -> code:add_pathz(filename:absname(?AUTO_GEN_CLIENT_SRC_DIR)) end,
        fun() -> code:add_pathz(filename:absname(?AUTO_GEN_CLIENT_EBIN_DIR)) end,
        fun() -> code:add_pathz(filename:absname(?AUTO_GEN_CLIENT_INCLUDE_DIR)) end,
        fun() -> code:add_pathz(filename:absname(?AUTO_GEN_MB_SRC_DIR)) end,
        fun() -> code:add_pathz(filename:absname(?AUTO_GEN_MB_EBIN_DIR)) end,
        fun() -> code:add_pathz(filename:absname(?AUTO_GEN_MB_INCLUDE_DIR)) end]).
    

            
            
