-module(mb_tables).
-include("../include/mb.hrl").

-export([update_ssg/1, unassign_self/1, unassign_worker/2]).


% INTERNAL_SSG_TABLE 
% MB_SSG_MODULE


update_ssg(SSG) -> 
    io:format("[mb::tables::update_ssg]: (...)"),
    case ssg_name_ok(SSG) of 
        true -> ?MB_SSG_MODULE:write_if(fun(ArgList) -> assign_ssg_ok(ArgList, self()) end, [SSG], {?INTERNAL_SSG_TABLE, maps:get(?NAME, SSG), SSG, self()});
        false -> {error, {internal_db_update_failed, {invalid_ssg_name, maps:get(?NAME, SSG)}}}
    end.

unassign_self(SSG) -> 
    io:format("[mb::tables::unassign_self]: (...)"),
    case ssg_name_ok(SSG) of 
        true -> ?MB_SSG_MODULE:write_if(fun(ArgList) -> unassign_ssg_ok(ArgList, self()) end, [SSG], {?INTERNAL_SSG_TABLE, maps:get(?NAME, SSG), SSG, []});
        false -> {error, {internal_db_update_failed, {invalid_ssg_name, maps:get(?NAME, SSG)}}}
    end.


unassign_worker(SSG, WorkerPid) -> 
    io:format("[mb::tables::unassign_worker]: (...)"),
    case ssg_name_ok(SSG) of 
        true -> ?MB_SSG_MODULE:write_if(fun(ArgList) -> unassign_ssg_ok(ArgList, WorkerPid) end, [SSG], {?INTERNAL_SSG_TABLE, maps:get(?NAME, SSG), SSG, []});
        false -> {error, {internal_db_update_failed, {invalid_ssg_name, maps:get(?NAME, SSG)}}}
    end.



%ssg_in_table(SSG) -> ok.

%delete_ssg(SSG) -> ok.

%add_ssg(SSG) -> ok.

%add_ssg_and_assign_self(SSG) -> ok.


assign_ssg_ok([SSG], Pid) -> 
    io:format("[mb::tables::assign_ssg_ok]: (...)"),
    Key = maps:get(?NAME, SSG),

    case ?MB_SSG_MODULE:read_no_trans(?INTERNAL_SSG_TABLE, Key) of 
        [] -> 
            io:format("[mb::tables::assign_ssg_ok]: no record, true"),
            true;
        [{Key, _, Pid}] -> 
            io:format("[mb::tables::assign_ssg_ok]: my pid ~p in record, true", [Pid]),
            true;
        [{Key, _, []}] -> 
            io:format("[mb::tables::assign_ssg_ok]: no pid, true"),
            true;
        [{Key, _, OtherPid}] -> 
            io:format("[mb::tables::assign_ssg_ok]: other worker ~p using record, false", [OtherPid]),
            false;
        _ -> false 
    end.



unassign_ssg_ok([SSG], Pid) -> 
    io:format("[mb::tables::assign_ssg_ok]: (...)"),
    Key = maps:get(?NAME, SSG),

    case ?MB_SSG_MODULE:read_no_trans(?INTERNAL_SSG_TABLE, Key) of 
        [] -> 
            io:format("[mb::tables::assign_ssg_ok]: no record, false"),
            false;
        [{Key, _, Pid}] -> 
            io:format("[mb::tables::assign_ssg_ok]: my pid ~p in record, true", [Pid]),
            true;
        [{Key, _, []}] -> 
            io:format("[mb::tables::assign_ssg_ok]: no pid, true"),
            true;
        [{Key, _, OtherPid}] -> 
            io:format("[mb::tables::assign_ssg_ok]: other worker ~p using record, false", [OtherPid]),
            false;
        _ -> false 
    end.


ssg_name_ok(SSG) -> 
    case maps:get(?NAME, SSG) of 
        [] -> false;
        ?DEFAULT_SSG_NAME -> fale;
        _ -> true
    end.
