-module(mb_tables).
-export([deploy_mb_ssg/0]).
-include("../include/mb.hrl").


%-------------------------------------------------------------
%   
%-------------------------------------------------------------
-spec deploy_mb_ssg() -> ok | mb_error().
%-------------------------------------------------------------
deploy_mb_ssg() ->
    
    Result =
        mb_utilities:chain_execution([
            fun() -> mb_ssg:new(mnesia_builder_ssg, "AllThingsErlang", "haitham.bouzeineddine@gmail.com", "MnesiaBuilder internal database") end,
            fun(SSG) -> mb_ssg:add_schema(ssg_table, SSG) end,
            fun(SSG) -> mb_ssg:set_schema_type(bag, ssg_table, SSG) end,
            fun(SSG) -> mb_ssg:add_schema_disc_copies([node()] ++ nodes(), ssg_table, SSG) end,

            fun(SSG) -> mb_ssg:add_field(name_version, ssg_table, SSG) end,
            fun(SSG) -> mb_ssg:set_field_type(tuple, name_version, ssg_table, SSG) end,

            fun(SSG) -> mb_ssg:add_field(state, ssg_table, SSG) end,
            fun(SSG) -> mb_ssg:set_field_type(atom, state, ssg_table, SSG) end,

            fun(SSG) -> mb_ssg:add_field(ssg, ssg_table, SSG) end,
            fun(SSG) -> mb_ssg:set_field_type(map, ssg, ssg_table, SSG) end,

            fun(SSG) -> mb_ssg:add_field(worker_pid, ssg_table, SSG) end,
            fun(SSG) -> mb_ssg:set_field_type(term, worker_pid, ssg_table, SSG) end,

            fun(SSG) -> mb_db_management:deploy(SSG) end
        ]),

    case Result of
        {ok, FinalResult} ->
            io:format("~n~n~p~n~n", [FinalResult]),
            io:format("[mb::mnesia_builder]: deploy result ~p~n", [FinalResult]),
            FinalResult;
        {error, Reason} ->
            io:format("[mb::mnesia_builder]: deployment failed ~p~n", [Reason]),
            {error, Reason}
    end.



