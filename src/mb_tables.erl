-module(mb_tables).
-include("../include/mb.hrl").

-export([assign_self/1, unassign_self/1]).


% INTERNAL_SSG_TABLE 
% MB_SSG_MODULE


assign_self(SSG) -> ?MB_SSG_MODULE:write_if(fun(ArgList) -> assign_ssg_ok(ArgList) end, [SSG], {?INTERNAL_SSG_TABLE, maps:get(?NAME, SSG), SSG, self()}).

unassign_self(SSG) -> ?MB_SSG_MODULE:write_exists({?INTERNAL_SSG_TABLE, maps:get(?NAME, SSG), SSG, []}).


%unassign_worker(WorkerPid) -> ok.


%ssg_in_table(SSG) -> ok.

%delete_ssg(SSG) -> ok.

%add_ssg(SSG) -> ok.

%add_ssg_and_assign_self(SSG) -> ok.


assign_ssg_ok([SSG]) -> 
    Key = maps:get(?NAME, SSG),
    MyPid = self(),

    case ?MB_SSG_MODULE:read_no_trans(?INTERNAL_SSG_TABLE, Key) of 
        [] -> 
            io:format("[mb::tables::assign_ssg_ok]: no record, true"),
            true;
        [{Key, _, MyPid}] -> 
            io:format("[mb::tables::assign_ssg_ok]: my pid ~p in record, true", [MyPid]),
            true;
        [{Key, _, []}] -> 
            io:format("[mb::tables::assign_ssg_ok]: no pid, true"),
            true;
        [{Key, _, OtherPid}] -> 
            io:format("[mb::tables::assign_ssg_ok]: other worker ~p using record, false", [OtherPid]),
            false;
        _ -> false 
    end.
