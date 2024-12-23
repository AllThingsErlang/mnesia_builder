-module(db_edit).
-include("../include/db_mnesia_builder.hrl").

-import(mnesia, [transaction/1]).
-export([add/2, add/3, add/4, delete/2, delete/3, clear_all_tables/1]).



%-------------------------------------------------------------
% Adds a record to the table. The table should have been 
% already created using the schema specifications. The function
% does validate against the schema to ensure that edit operations
% are in compliance with schema specifications.
%
% Three formats for the add ....
%
%    (1) add({TableName, Key, ...}, SS)
%    (2) add(TableName, {Key, ...}, SS)
%    (3) add(TableName, Key, Data, SS) 
%
%-------------------------------------------------------------
-spec add(tuple(), map()) -> db_result().
%-------------------------------------------------------------
add(Record, SS) when (is_tuple(Record) and is_map(SS)) ->

    List = tuple_to_list(Record),
    
    case length(List) > 2 of
        % There has to be at least 3 elements: table name, key, and one data
        true ->
            [TableName | RestOfRecord] = Record,
            [Key | Data] = RestOfRecord,

            add(TableName, Key, Data, SS);

        false -> {error, {invalid_record, Record}}
    end;

add(Record, SS) -> {error, {invalid_arguments, {Record, SS}}}.


%-------------------------------------------------------------
-spec add(atom(), tuple(), map()) -> db_result().
%-------------------------------------------------------------
add(TableName, Record, SS) when (is_atom(TableName) and is_tuple(Record) and is_map(SS)) ->

    List = tuple_to_list(Record),
    
    case length(List) > 1 of
        true ->
            [Key | Data] = Record,
            add(TableName, Key, Data, SS);

        false -> {error, {invalid_record, Record}}
    end;

add(TableName, Record, SS) -> {error, {invalid_arguments, {TableName, Record, SS}}}.

%-------------------------------------------------------------
-spec add(atom(), term(), term(), map()) -> db_result().
%-------------------------------------------------------------
add(TableName, Key, Data, SS) when (is_atom(TableName) and 
                                    is_tuple(Data) and 
                                    is_map (SS)) ->

    % 1. Validate that TableName is in SS
    case db_schemas:is_schema(TableName, SS) of 
        true -> 
            % 2. Execute type checks (TODO)

            % 3. Complete the operation
            Record = list_to_tuple([TableName | [Key | tuple_to_list(Data)]]),

            case mnesia:transaction(fun() -> mnesia:write(Record) end) of
                {atomic, ok} -> ok;
                {aborted, Reason} -> {error, Reason}
            end;

        false -> {error, {invalid_schema_name, TableName}}
    end;

add(TableName, Key, Data, SS) -> {error, {invalid_arguments, {TableName, Key, Data, SS}}}.


%-------------------------------------------------------------
% Deletes a record from the table Table whose key is Key. The
% table should have been already created using the schema
% specifications. Validation against the schema specifications
% are applied to ensure that the delete operation remains
% within the domain of the specifications.
%-------------------------------------------------------------
-spec delete(tuple(), map()) -> db_result().
%-------------------------------------------------------------
delete({TableName, Key}, SS) when is_map(SS) -> delete(TableName, Key, SS);
delete(TableKey, SS) -> {error, {invalid_arguments, {TableKey, SS}}}.

%-------------------------------------------------------------
-spec delete(atom(), term(), map()) -> db_result().
%-------------------------------------------------------------
delete(TableName, Key, SS) when (is_atom(TableName) and is_map(SS)) -> 
    
   case db_schemas:is_schema(TableName, SS) of 
        true -> 
            
            Fun = fun() -> mnesia:delete({TableName, Key}) end,

            case mnesia:transaction(Fun) of
                {atomic, ok} -> ok;
                {aborted, Reason} -> {error, Reason}
            end;

        false -> {error, {invalid_schema_name, TableName}}
    end;

delete(TableName, Key, SS) -> {error, {invalid_arguments, {TableName, Key, SS}}}.



%-------------------------------------------------------------
% Deletes data from all the tables defined by the specifications.
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
