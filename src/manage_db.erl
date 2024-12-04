-module(manage_db).
-include("../include/schemas.hrl").

-export([install/1, install/2, start/0, stop/0, table_sizes/0, table_size/1]).


install(SS) -> install([node()], SS).

install(NodeList, SS) ->
    
    case mnesia:create_schema(NodeList) of
        ok -> ok;
        {error, Reason1} -> io:format("failed to create get_schema: ~w~n", [Reason1])
    end,

    mnesia:start(),
    
    SchemaNames = schemas:schema_names(SS),

    install_next(SchemaNames, SS).


install_next([], _) -> ok;
install_next([NextSchemaName | T], SS) ->

    case mnesia:create_table(NextSchemaName, [{attributes, schemas:field_names(NextSchemaName, SS)},
                             {type, schemas:get_schema_attribute(type, NextSchemaName,SS)},
                             {disc_copies, schemas:get_schema_attribute(disc_copies, NextSchemaName,SS)},
                             {disc_only_copies, schemas:get_schema_attribute(disc_only_copies, NextSchemaName, SS)},
                             {ram_copies, schemas:get_schema_attribute(ram_copies, NextSchemaName, SS)}]) of

        {atomic, _} -> install_next(T, SS);
        {aborted, Reason2} -> io:format("failed to create table ~p~n", [Reason2])
    end.


start() ->
    mnesia:start(),
    mnesia:wait_for_tables(schemas:get_tables(), 10000).
 
stop() -> mnesia:stop().



%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
table_size(Table) -> mnesia:table_info(Table, size).

table_sizes() -> table_sizes(schemas:get_tables()).

table_sizes([]) -> [];
table_sizes(Tables) -> table_sizes(Tables, []).

table_sizes([], Sizes) -> lists:reverse(Sizes);
table_sizes([H|T], Sizes) -> table_sizes(T, [{H, table_size(H)} | Sizes]).

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
