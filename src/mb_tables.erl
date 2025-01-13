-module(mb_tables).
-include("../include/mb.hrl").

-export([update_ssg/1, unassign_self/1, unassign_worker/2, unassign_all_workers/0]).


% INTERNAL_SSG_TABLE 
% MB_SSG_MODULE


update_ssg(SSG) -> 
    io:format("[mb::tables::update_ssg]: (...)~n"),
    case ssg_name_ok(SSG) of 
        true -> ?MB_SSG_MODULE:write_if(fun(ArgList) -> assign_ssg_ok(ArgList, self()) end, [SSG], {?INTERNAL_SSG_TABLE, maps:get(?NAME, SSG), SSG, self()});
        false -> {error, {internal_db_update_failed, {invalid_ssg_name, maps:get(?NAME, SSG)}}}
    end.

unassign_self(SSG) -> 
    io:format("[mb::tables::unassign_self]: (...)~n"),
    io:format("[mb::tables::unassign_self]: ~p~n", [maps:get(?NAME, SSG)]),
    case ssg_name_ok(SSG) of 
        true -> ?MB_SSG_MODULE:write_if(fun(ArgList) -> unassign_ssg_ok(ArgList, self()) end, [SSG], {?INTERNAL_SSG_TABLE, maps:get(?NAME, SSG), SSG, []});
        false -> {error, {internal_db_update_failed, {invalid_ssg_name, maps:get(?NAME, SSG)}}}
    end.


unassign_worker(SSG, WorkerPid) -> 
    io:format("[mb::tables::unassign_worker]: (...)~n"),
    case ssg_name_ok(SSG) of 
        true -> ?MB_SSG_MODULE:write_if(fun(ArgList) -> unassign_ssg_ok(ArgList, WorkerPid) end, [SSG], {?INTERNAL_SSG_TABLE, maps:get(?NAME, SSG), SSG, []});
        false -> {error, {internal_db_update_failed, {invalid_ssg_name, maps:get(?NAME, SSG)}}}
    end.

unassign_all_workers() -> 
    List = ?MB_SSG_MODULE:select(ssg_table, worker_pid, '/=', []),
    force_remove_workers(List).



%ssg_in_table(SSG) -> ok.

%delete_ssg(SSG) -> ok.

%add_ssg(SSG) -> ok.

%add_ssg_and_assign_self(SSG) -> ok.


assign_ssg_ok([SSG], Pid) -> 
    io:format("[mb::tables::assign_ssg_ok]: (...)~n"),
    Key = maps:get(?NAME, SSG),

    case ?MB_SSG_MODULE:read_no_trans(?INTERNAL_SSG_TABLE, Key) of 
        [] -> 
            io:format("[mb::tables::assign_ssg_ok]: no record, true~n"),
            true;
        [{?INTERNAL_SSG_TABLE, Key, _, Pid}] -> 
            io:format("[mb::tables::assign_ssg_ok]: my pid ~p in record, true~n", [Pid]),
            true;
        [{?INTERNAL_SSG_TABLE, Key, _, []}] -> 
            io:format("[mb::tables::assign_ssg_ok]: no pid, true~n"),
            true;
        [{?INTERNAL_SSG_TABLE, Key, _, OtherPid}] -> 
            io:format("[mb::tables::assign_ssg_ok]: other worker ~p using record, false~n", [OtherPid]),
            false;
        Other ->
            io:format("[mb::tables::assign_ssg_ok]: ~p, false", [Other]), 
            false 
    end.



unassign_ssg_ok([SSG], Pid) -> 
    io:format("[mb::tables::assign_ssg_ok]: (...)~n"),
    Key = maps:get(?NAME, SSG),

    case ?MB_SSG_MODULE:read_no_trans(?INTERNAL_SSG_TABLE, Key) of 
        [] -> 
            io:format("[mb::tables::assign_ssg_ok]: no record, false~n"),
            false;
        [{?INTERNAL_SSG_TABLE, Key, _, Pid}] -> 
            io:format("[mb::tables::assign_ssg_ok]: my pid ~p in record, true~n", [Pid]),
            true;
        [{?INTERNAL_SSG_TABLE, Key, _, []}] -> 
            io:format("[mb::tables::assign_ssg_ok]: no pid, true~n"),
            true;
        [{?INTERNAL_SSG_TABLE, Key, _, OtherPid}] -> 
            io:format("[mb::tables::assign_ssg_ok]: other worker ~p using record, false~n", [OtherPid]),
            false;
        Other ->
            io:format("[mb::tables::assign_ssg_ok]: ~p, false", [Other]), 
            false 
    end.


ssg_name_ok(SSG) -> 
    case maps:get(?NAME, SSG) of 
        [] -> false;
        ?DEFAULT_SSG_NAME -> false;
        _ -> true
    end.


force_remove_workers([]) -> ok;
force_remove_workers([{?INTERNAL_SSG_TABLE, _Name, SSG, WorkerPid} | Rest]) -> 
    ?MB_SSG_MODULE:write_if(fun(ArgList) -> unassign_ssg_ok(ArgList, WorkerPid) end, [SSG], {?INTERNAL_SSG_TABLE, maps:get(?NAME, SSG), SSG, []}),
    force_remove_workers(Rest).

