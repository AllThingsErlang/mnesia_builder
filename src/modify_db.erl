-module(modify_db).
-import(mnesia, [transaction/1]).

-export([add/1, delete/2, clear_all_tables/1]).


%-------------------------------------------------------------
% Function: add
% Purpose:  Adds a record to the specified table
% Returns: ok | {error, Reason}
%-------------------------------------------------------------
add(Record) when is_tuple(Record) ->
    
    case mnesia:transaction(fun() -> mnesia:write(Record) end) of
        {atomic, ok} -> ok;
        {aborted, Reason} -> {error, Reason}
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
clear_all_tables(SS) ->

    Tables = schemas:schema_names(SS),
    clear_all_tables_next(Tables).

clear_all_tables_next(Tables) -> clear_all_tables_next(Tables, 0, 0).

clear_all_tables_next([], Cleared, NotCleared) -> {Cleared, NotCleared};
clear_all_tables_next([H|T], Cleared, NotCleared) ->

    case mnesia:clear_table(H) of
        {atomic, _} -> 
            NewCleared = Cleared + 1,
            NewNotCleared = NotCleared;

        {aborted, _} -> 
            NewCleared = Cleared,
            NewNotCleared = NotCleared + 1
    end,

    clear_all_tables_next(T, NewCleared, NewNotCleared).
