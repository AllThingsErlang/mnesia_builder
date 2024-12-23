-module(db_management).

-export([install/1, install/2, start/1, stop/0, table_sizes/1, table_size/1]).


install(SS) -> install([node()], SS).

install(NodeList, SS) ->
    
    case mnesia:create_schema(NodeList) of
        ok -> ok;
        {error, Reason1} -> io:format("failed to create get_schema: ~w~n", [Reason1])
    end,

    mnesia:start(),
    
    SchemaNames = db_schemas:schema_names(SS),

    install_next(SchemaNames, SS).


install_next([], _) -> ok;
install_next([NextSchemaName | T], SS) ->

    case mnesia:create_table(NextSchemaName, [{attributes, db_schemas:field_names(NextSchemaName, SS)},
                             {type, db_schemas:get_schema_attribute(type, NextSchemaName,SS)},
                             {disc_copies, db_schemas:get_schema_attribute(disc_copies, NextSchemaName,SS)},
                             {disc_only_copies, db_schemas:get_schema_attribute(disc_only_copies, NextSchemaName, SS)},
                             {ram_copies, db_schemas:get_schema_attribute(ram_copies, NextSchemaName, SS)}]) of

        {atomic, _} -> install_next(T, SS);
        {aborted, Reason2} -> io:format("failed to create table ~p~n", [Reason2])
    end.


start(SS) ->
    mnesia:start(),
    mnesia:wait_for_tables(db_schemas:schema_names(SS), 10000).
 
stop() -> mnesia:stop().



%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
table_size(Table) -> mnesia:table_info(Table, size).

table_sizes(SS) -> table_sizes_next(db_schemas:schema_names(SS)).

table_sizes_next([]) -> [];
table_sizes_next(Tables) -> table_sizes_next(Tables, []).

table_sizes_next([], Sizes) -> lists:reverse(Sizes);
table_sizes_next([H|T], Sizes) -> table_sizes_next(T, [{H, table_size(H)} | Sizes]).

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
