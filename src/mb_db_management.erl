-module(mb_db_management).
-include("../include/mb.hrl").

-export([install/1, install/2, start/1, stop/0, table_sizes/1, table_size/2]).


%-------------------------------------------------------------
% Installs the mnesia schema and then creates the tables define
% in the schema specifications. The table attributes need to be 
% completed in the schema specifications.
%
% Two variants:
%    install/1: installs mnesia schema on the local node only.
%    install/2: installs mnesia schema on the nodes provided in NodeList.
%
% TODO: 
%     - Make the install intelligent by extracting all the nodes
%       from SS and then installing the schema on them.
%     - Validate the SS before starting the install.
%     - If there is an mnesia schema installed, change its config
%       to support the expanded nodes list.
%-------------------------------------------------------------
-spec install(map()) -> mb_result().
%-------------------------------------------------------------
install(SS) -> install([node()], SS).


%-------------------------------------------------------------
-spec install(list(), map()) -> mb_result().
%-------------------------------------------------------------
install(NodeList, SS) ->
    
    case mnesia:create_schema(NodeList) of
        ok -> ok;
        {error, Reason1} -> io:format("failed to create get_schema: ~w~n", [Reason1])
    end,

    mnesia:start(),
    
    SchemaNames = mb_schemas:schema_names(SS),

    install_next(SchemaNames, SS).


install_next([], _) -> ok;
install_next([NextSchemaName | T], SS) ->

    case mnesia:create_table(NextSchemaName, [{attributes, mb_schemas:field_names(NextSchemaName, SS)},
                             {type, mb_schemas:get_schema_attribute(type, NextSchemaName,SS)},
                             {disc_copies, mb_schemas:get_schema_attribute(disc_copies, NextSchemaName,SS)},
                             {disc_only_copies, mb_schemas:get_schema_attribute(disc_only_copies, NextSchemaName, SS)},
                             {ram_copies, mb_schemas:get_schema_attribute(ram_copies, NextSchemaName, SS)}]) of

        {atomic, _} -> install_next(T, SS);
        {aborted, Reason2} -> io:format("failed to create table ~p~n", [Reason2])
    end.


%-------------------------------------------------------------
-spec start(map()) -> ok | {error, term()} | {timeout, [atom()]}.
%-------------------------------------------------------------
start(SS) ->
    mnesia:start(),
    mnesia:wait_for_tables(mb_schemas:schema_names(SS), 10000).
 
stop() -> mnesia:stop().



%-------------------------------------------------------------
% Returns the number of records in the specified table. 
%-------------------------------------------------------------
-spec table_size(atom(), map()) -> integer().
%-------------------------------------------------------------
table_size(SchemaName, SS) -> 
    
    case mb_schemas:is_schema(SchemaName, SS) of 
        true -> mnesia:table_info(SchemaName, size);
        false -> {error, {invalid_schema_name, SchemaName}}
    end.


%-------------------------------------------------------------
% Returns a list of tuples [{SchemaName, TableSize}]. 
%-------------------------------------------------------------
-spec table_sizes(map()) -> list().
%-------------------------------------------------------------
table_sizes(SS) -> table_sizes_next(mb_schemas:schema_names(SS)).

table_sizes_next([]) -> [];
table_sizes_next(Tables) -> table_sizes_next(Tables, []).

table_sizes_next([], Sizes) -> lists:reverse(Sizes);
table_sizes_next([H|T], Sizes) -> table_sizes_next(T, [{H, mnesia:table_info(H, size)} | Sizes]).


