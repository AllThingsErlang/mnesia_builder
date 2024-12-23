-module(db_edit).
-include("../include/db_mnesia_builder.hrl").

-import(mnesia, [transaction/1]).
-export([add/1, delete/2, clear_all_tables/1]).



%-------------------------------------------------------------
% Adds a record to the table. The record must be in the form
% {table_name, key, field, ...}. The table should have been 
% already created using the schema specifications.
%-------------------------------------------------------------
-spec add(tuple()) -> db_result().
%-------------------------------------------------------------
add(Record) when is_tuple(Record) ->
    
    case mnesia:transaction(fun() -> mnesia:write(Record) end) of
        {atomic, ok} -> ok;
        {aborted, Reason} -> {error, Reason}
    end;

add(_) -> {error, not_a_tuple}.


%-------------------------------------------------------------
% Deletes a record from the table Table whose key is Key. The
% table should have been already created using the schema
% specifications.
%-------------------------------------------------------------
-spec delete(atom(), term()) -> db_result().
%-------------------------------------------------------------
delete(Table, Key) -> 
    
   Fun = fun() -> mnesia:delete({Table, Key}) end,

    case mnesia:transaction(Fun) of
        {atomic, ok} -> ok;
        {aborted, Reason} -> {error, Reason}
    end.


%-------------------------------------------------------------
% Deletes all the tables defined by the given specifications.
% The tables should have been created previously. It returns
% a tuple of two lists: {ClearedTables, NotClearedTables}.
%-------------------------------------------------------------
-spec clear_all_tables(map()) -> {list(), list()}.
%-------------------------------------------------------------
clear_all_tables(SS) ->

    Tables = db_schemas:schema_names(SS),
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
