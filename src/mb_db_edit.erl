-module(mb_db_edit).
-include("../include/mb.hrl").

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
%    (1) add({SchemaName, Key, ...}, SSG)
%    (2) add(SchemaName, {Key, ...}, SSG)
%    (3) add(SchemaName, Key, Data, SSG) 
%
%-------------------------------------------------------------
-spec add(tuple(), map()) -> mb_result().
%-------------------------------------------------------------
add(Record, SSG) when (is_tuple(Record) and is_map(SSG)) ->

    List = tuple_to_list(Record),
    
    case length(List) > 2 of
        % There has to be at least 3 elements: table name, key, and one data
        true ->
            [SchemaName | RestOfRecord] = Record,
            [Key | Data] = RestOfRecord,

            add(SchemaName, Key, Data, SSG);

        false -> {error, {invalid_record, Record}}
    end;

add(Record, SSG) -> {error, {invalid_argument, {Record, SSG}}}.


%-------------------------------------------------------------
-spec add(atom(), tuple(), map()) -> mb_result().
%-------------------------------------------------------------
add(SchemaName, Record, SSG) when (is_atom(SchemaName) and is_tuple(Record) and is_map(SSG)) ->

    List = tuple_to_list(Record),
    
    case length(List) > 1 of
        true ->
            [Key | Data] = Record,
            add(SchemaName, Key, Data, SSG);

        false -> {error, {invalid_record, Record}}
    end;

add(SchemaName, Record, SSG) -> {error, {invalid_argument, {SchemaName, Record, SSG}}}.

%-------------------------------------------------------------
-spec add(atom(), term(), term(), map()) -> mb_result().
%-------------------------------------------------------------
add(SchemaName, Key, Data, SSG) when (is_atom(SchemaName) and 
                                    is_tuple(Data) and 
                                    is_map (SSG)) ->

    % 1. Validate that SchemaName is in SSG
    case mb_schemas:is_schema(SchemaName, SSG) of 
        true -> 
            % 2. Execute type checks (TODO)

            % 3. Complete the operation
            Record = list_to_tuple([SchemaName | [Key | tuple_to_list(Data)]]),

            case mnesia:transaction(fun() -> mnesia:write(Record) end) of
                {atomic, ok} -> ok;
                {aborted, Reason} -> {error, Reason}
            end;

        false -> {error, {invalid_schema_name, SchemaName}}
    end;

add(SchemaName, Key, Data, SSG) -> {error, {invalid_argument, {SchemaName, Key, Data, SSG}}}.


%-------------------------------------------------------------
% Deletes a record from the table SchemaName whose key is Key. The
% table should have been already created using the schema
% specifications. Validation against the schema specifications
% are applied to ensure that the delete operation remains
% within the domain of the specifications.
%-------------------------------------------------------------
-spec delete(tuple(), map()) -> mb_result().
%-------------------------------------------------------------
delete({SchemaName, Key}, SSG) when is_map(SSG) -> delete(SchemaName, Key, SSG);
delete(TableKey, SSG) -> {error, {invalid_argument, {TableKey, SSG}}}.

%-------------------------------------------------------------
-spec delete(atom(), term(), map()) -> mb_result().
%-------------------------------------------------------------
delete(SchemaName, Key, SSG) when (is_atom(SchemaName) and is_map(SSG)) -> 
    
   case mb_schemas:is_schema(SchemaName, SSG) of 
        true -> 
            
            Fun = fun() -> mnesia:delete({SchemaName, Key}) end,

            case mnesia:transaction(Fun) of
                {atomic, ok} -> ok;
                {aborted, Reason} -> {error, Reason}
            end;

        false -> {error, {invalid_schema_name, SchemaName}}
    end;

delete(SchemaName, Key, SSG) -> {error, {invalid_argument, {SchemaName, Key, SSG}}}.



%-------------------------------------------------------------
% Deletes data from all the tables defined by the specifications.
% The tables should have been created previously. It returns
% a tuple of two lists: {ClearedTables, NotClearedTables}.
%-------------------------------------------------------------
-spec clear_all_tables(map()) -> {list(), list()}.
%-------------------------------------------------------------
clear_all_tables(SSG) ->

    Tables = mb_schemas:schema_names(SSG),
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
