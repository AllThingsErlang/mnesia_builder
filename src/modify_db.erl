-module(modify_db).
-import(mnesia, [transaction/1]).

-include("../include/schemas.hrl").

-export([add/4, delete/2, clear_all_tables/0]).


%-------------------------------------------------------------
% Function: add
% Purpose:  Adds a record to the specified table
% Returns: ok | {error, Reason}
%-------------------------------------------------------------
add(SS, Table, Key, Data) ->

    case schemas:build_record(Table, Key, Data) of
        {ok, Record} ->
            case mnesia:transaction(fun() -> mnesia:write(Record) end) of
                {atomic, ok} -> ok;
                {aborted, Reason} -> {error, Reason}
            end;

        {error, Reason} -> {error, Reason}
    end.


%-------------------------------------------------------------
% Function: delete
% Purpose:  Deletes a record form the schemas table
% Returns:  ok | {error, Reason}
%-------------------------------------------------------------
delete(Table, Key) -> 
    
   Fun = fun() -> mnesia:delete({Table, Key}) end,

    case mnesia:transaction(Fun) of
        {atomic, ok} -> ok;
        {aborted, Reason} -> {error, Reason}
    end.


%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
clear_all_tables() ->

    Tables = schemas:get_tables(),
    clear_all_tables(Tables).

clear_all_tables(Tables) -> clear_all_tables(Tables, 0, 0).

clear_all_tables([], Cleared, NotCleared) -> {Cleared, NotCleared};
clear_all_tables([H|T], Cleared, NotCleared) ->

    case mnesia:clear_table(H) of
        {atomic, _} -> 
            NewCleared = Cleared + 1,
            NewNotCleared = NotCleared;

        {aborted, _} -> 
            NewCleared = Cleared,
            NewNotCleared = NotCleared + 1
    end,

    clear_all_tables(T, NewCleared, NewNotCleared).
