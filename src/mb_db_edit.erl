-module(mb_db_edit).
-include("../include/mb.hrl").

-import(mnesia, [transaction/1]).
-export([write/2, write/3, write/4, 
         write_new/2, write_new/3, write_new/4,
         write_exists/2, write_exists/3, write_exists/4,
         write_if/4, write_if/5, write_if/6,
         delete/2, delete/3, clear_all_tables/1]).



%-------------------------------------------------------------
% Adds a record to the table. The table should have been 
% already created using the schema specifications. The function
% does validate against the schema to ensure that edit operations
% are in compliance with schema specifications.
%
% Three formats for the write ....
%
%    (1) write({SchemaName, Key, ...}, SSG)
%    (2) write(SchemaName, {Key, ...}, SSG)
%    (3) write(SchemaName, Key, Data, SSG) 
%
%-------------------------------------------------------------
-spec write(tuple(), map()) -> ok | mb_error().
%-------------------------------------------------------------
write(Record, SSG) when (is_tuple(Record) and is_map(SSG)) ->

    List = tuple_to_list(Record),
    
    case length(List) > 2 of
        % There has to be at least 3 elements: table name, key, and one data
        true ->
            [SchemaName | RestOfRecord] = List,
            [Key | Data] = RestOfRecord,

            write(SchemaName, Key, list_to_tuple(Data), SSG);

        false -> {error, {invalid_record, Record}}
    end;

write(Record, SSG) -> {error, {invalid_argument, {Record, SSG}}}.


%-------------------------------------------------------------
-spec write(atom(), tuple(), map()) -> ok | mb_error().
%-------------------------------------------------------------
write(SchemaName, Record, SSG) when (is_atom(SchemaName) and is_tuple(Record) and is_map(SSG)) ->

    List = tuple_to_list(Record),
    
    case length(List) > 1 of
        true ->
            [Key | Data] = List,
            write(SchemaName, Key, list_to_tuple(Data), SSG);

        false -> {error, {invalid_record, Record}}
    end;

write(SchemaName, Record, SSG) -> {error, {invalid_argument, {SchemaName, Record, SSG}}}.

%-------------------------------------------------------------
-spec write(atom(), term(), term(), map()) -> ok | mb_error().
%-------------------------------------------------------------
write(SchemaName, Key, Data, SSG) when (is_atom(SchemaName) and 
                                    is_tuple(Data) and 
                                    is_map (SSG)) ->

    % 1. Validate that SchemaName is in SSG
    case mb_ssg:is_schema(SchemaName, SSG) of 
        true -> 
            % 2. Execute type checks (TODO)

            % 3. Complete the operation
            Record = list_to_tuple([SchemaName | [Key | tuple_to_list(Data)]]),

            case mnesia:transaction(fun() -> mnesia:write(Record) end) of
                {atomic, ok} -> ok;
                {_, Reason} -> {error, Reason}
            end;

        false -> {error, {invalid_schema_name, SchemaName}}
    end;

write(SchemaName, Key, Data, SSG) -> {error, {invalid_argument, {SchemaName, Key, Data, SSG}}}.


%-------------------------------------------------------------
% 
%
%-------------------------------------------------------------
-spec write_new(tuple(), map()) -> ok | mb_error().
%-------------------------------------------------------------
write_new(Record, SSG) when (is_tuple(Record) and is_map(SSG)) ->

    List = tuple_to_list(Record),
    
    case length(List) > 2 of
        % There has to be at least 3 elements: table name, key, and one data
        true ->
            [SchemaName | RestOfRecord] = List,
            [Key | Data] = RestOfRecord,

            write_new(SchemaName, Key, list_to_tuple(Data), SSG);

        false -> {error, {invalid_record, Record}}
    end;

write_new(Record, SSG) -> {error, {invalid_argument, {Record, SSG}}}.


%-------------------------------------------------------------
-spec write_new(atom(), tuple(), map()) -> ok | mb_error().
%-------------------------------------------------------------
write_new(SchemaName, Record, SSG) when (is_atom(SchemaName) and is_tuple(Record) and is_map(SSG)) ->

    List = tuple_to_list(Record),
    
    case length(List) > 1 of
        true ->
            [Key | Data] = List,
            write_new(SchemaName, Key, list_to_tuple(Data), SSG);

        false -> {error, {invalid_record, Record}}
    end;

write_new(SchemaName, Record, SSG) -> {error, {invalid_argument, {SchemaName, Record, SSG}}}.

%-------------------------------------------------------------
-spec write_new(atom(), term(), term(), map()) -> ok | mb_error().
%-------------------------------------------------------------
write_new(SchemaName, Key, Data, SSG) when (is_atom(SchemaName) and 
                                            is_tuple(Data) and 
                                            is_map (SSG)) ->

    % 1. Validate that SchemaName is in SSG
    case mb_ssg:is_schema(SchemaName, SSG) of 
        true -> 
            % 2. Execute type checks (TODO)

            % 3. build the function to be used by the transaction
            %    First, make sure that the key does not exists.
            %    Then write.
            Fun = fun() -> 
                case mnesia:read({SchemaName, Key}) of
                    [] -> 
                        Record = list_to_tuple([SchemaName | [Key | tuple_to_list(Data)]]),
                        mnesia:write(Record);
                    _ -> {error, key_exists}
                end
            end,

            case mnesia:transaction(Fun) of
                {atomic, ok} -> ok;
                {_, Reason} -> {error, Reason}
            end;

        false -> {error, {invalid_schema_name, SchemaName}}
    end;

write_new(SchemaName, Key, Data, SSG) -> {error, {invalid_argument, {SchemaName, Key, Data, SSG}}}.


%-------------------------------------------------------------
% 
%
%-------------------------------------------------------------
-spec write_exists(tuple(), map()) -> ok | mb_error().
%-------------------------------------------------------------
write_exists(Record, SSG) when (is_tuple(Record) and is_map(SSG)) ->

    List = tuple_to_list(Record),
    
    case length(List) > 2 of
        % There has to be at least 3 elements: table name, key, and one data
        true ->
            io:format("~n~n~p~n~n", [List]),
            [SchemaName | RestOfRecord] = List,
            [Key | Data] = RestOfRecord,

            write_exists(SchemaName, Key, list_to_tuple(Data), SSG);

        false -> {error, {invalid_record, Record}}
    end;

write_exists(Record, SSG) -> {error, {invalid_argument, {Record, SSG}}}.


%-------------------------------------------------------------
-spec write_exists(atom(), tuple(), map()) -> ok | mb_error().
%-------------------------------------------------------------
write_exists(SchemaName, Record, SSG) when (is_atom(SchemaName) and is_tuple(Record) and is_map(SSG)) ->

    List = tuple_to_list(Record),
    
    case length(List) > 1 of
        true ->
            [Key | Data] = List,
            write_exists(SchemaName, Key, list_to_tuple(Data), SSG);

        false -> {error, {invalid_record, Record}}
    end;

write_exists(SchemaName, Record, SSG) -> {error, {invalid_argument, {SchemaName, Record, SSG}}}.

%-------------------------------------------------------------
-spec write_exists(atom(), term(), term(), map()) -> ok | mb_error().
%-------------------------------------------------------------
write_exists(SchemaName, Key, Data, SSG) when (is_atom(SchemaName) and 
                                                is_tuple(Data) and 
                                                is_map (SSG)) ->

    % 1. Validate that SchemaName is in SSG
    case mb_ssg:is_schema(SchemaName, SSG) of 
        true -> 
            % 2. Execute type checks (TODO)

            % 3. build the function to be used by the transaction
            %    First, make sure that the key does not exists.
            %    Then write.
            Fun = fun() -> 
                case mnesia:read({SchemaName, Key}) of
                    [] -> {error, key_not_found};
                    _ ->
                        Record = list_to_tuple([SchemaName | [Key | tuple_to_list(Data)]]),
                        mnesia:write(Record)
                end
            end,

            case mnesia:transaction(Fun) of
                {atomic, ok} -> ok;
                {atomic, {error,Reason}} -> {error,Reason};
                {_, Reason} -> {error, Reason}
            end;

        false -> {error, {invalid_schema_name, SchemaName}}
    end;

write_exists(SchemaName, Key, Data, SSG) -> {error, {invalid_argument, {SchemaName, Key, Data, SSG}}}.



%-------------------------------------------------------------
% 
%
%-------------------------------------------------------------
-spec write_if(fun(), list(), tuple(), map()) -> ok | mb_error().
%-------------------------------------------------------------
write_if(CheckFun, ArgList, Record, SSG) when (is_tuple(Record) and is_map(SSG)) ->

    List = tuple_to_list(Record),
    
    case length(List) > 2 of
        % There has to be at least 3 elements: table name, key, and one data
        true ->
            io:format("~n~n~p~n~n", [List]),
            [SchemaName | RestOfRecord] = List,
            [Key | Data] = RestOfRecord,

            write_if(CheckFun, ArgList, SchemaName, Key, list_to_tuple(Data), SSG);

        false -> {error, {invalid_record, Record}}
    end;

write_if(_, _, Record, SSG) -> {error, {invalid_argument, {Record, SSG}}}.


%-------------------------------------------------------------
-spec write_if(fun(), list(), atom(), tuple(), map()) -> ok | mb_error().
%-------------------------------------------------------------
write_if(CheckFun, ArgList, SchemaName, Record, SSG) when (is_atom(SchemaName) and is_tuple(Record) and is_map(SSG)) ->

    List = tuple_to_list(Record),
    
    case length(List) > 1 of
        true ->
            [Key | Data] = List,
            write_if(CheckFun, ArgList, SchemaName, Key, list_to_tuple(Data), SSG);

        false -> {error, {invalid_record, Record}}
    end;

write_if(_, _, SchemaName, Record, SSG) -> {error, {invalid_argument, {SchemaName, Record, SSG}}}.

%-------------------------------------------------------------
-spec write_if(fun(), list(), atom(), term(), term(), map()) -> ok | mb_error().
%-------------------------------------------------------------
write_if(CheckFun, ArgList, SchemaName, Key, Data, SSG) when (is_atom(SchemaName) and 
                                                is_tuple(Data) and 
                                                is_map (SSG)) ->

    % 1. Validate that SchemaName is in SSG
    case mb_ssg:is_schema(SchemaName, SSG) of 
        true -> 
            % 2. Execute type checks (TODO)

            % 3. build the function to be used by the transaction
            %    First, make sure that the key does not exists.
            %    Then write.
            Fun = fun() -> 
                case CheckFun(ArgList) of 
                    true -> 
                        Record = list_to_tuple([SchemaName | [Key | tuple_to_list(Data)]]),
                        mnesia:write(Record);

                    false -> {error, condition_failed}
                end
            end,

            case mnesia:transaction(Fun) of
                {atomic, ok} -> ok;
                {atomic, {error,Reason}} -> {error,Reason};
                {_, Reason} -> {error, Reason}
            end;

        false -> {error, {invalid_schema_name, SchemaName}}
    end;

write_if(_, _, SchemaName, Key, Data, SSG) -> {error, {invalid_argument, {SchemaName, Key, Data, SSG}}}.


%-------------------------------------------------------------
% Deletes a record from the table SchemaName whose key is Key. The
% table should have been already created using the schema
% specifications. Validation against the schema specifications
% are applied to ensure that the delete operation remains
% within the domain of the specifications.
%-------------------------------------------------------------
-spec delete(tuple(), map()) -> ok | mb_error().
%-------------------------------------------------------------
delete({SchemaName, Key}, SSG) when is_map(SSG) -> delete(SchemaName, Key, SSG);
delete(TableKey, SSG) -> {error, {invalid_argument, {TableKey, SSG}}}.

%-------------------------------------------------------------
-spec delete(atom(), term(), map()) -> ok | mb_error().
%-------------------------------------------------------------
delete(SchemaName, Key, SSG) when (is_atom(SchemaName) and is_map(SSG)) -> 
    
   case mb_ssg:is_schema(SchemaName, SSG) of 
        true -> 
            
            Fun = fun() -> mnesia:delete({SchemaName, Key}) end,

            case mnesia:transaction(Fun) of
                {atomic, ok} -> ok;
                {_, Reason} -> {error, Reason}
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

    Tables = mb_ssg:schema_names(SSG),
    clear_all_tables_next(Tables).

clear_all_tables_next(Tables) -> clear_all_tables_next(Tables, 0, 0).

clear_all_tables_next([], Cleared, NotCleared) -> {Cleared, NotCleared};
clear_all_tables_next([H|T], Cleared, NotCleared) ->

    case mnesia:clear_table(H) of
        {atomic, _} -> 
            NewCleared = Cleared + 1,
            NewNotCleared = NotCleared;

        _ -> 
            NewCleared = Cleared,
            NewNotCleared = NotCleared + 1
    end,

    clear_all_tables_next(T, NewCleared, NewNotCleared).
