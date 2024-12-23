-module(db_management).
-include("../include/db_mnesia_builder.hrl").

-export([install/1, install/2, start/1, stop/0, table_sizes/1, table_size/1]).


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
-spec install(map()) -> db_result().
%-------------------------------------------------------------
install(SS) -> install([node()], SS).


%-------------------------------------------------------------
-spec install(list(), map()) -> db_result().
%-------------------------------------------------------------
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


%-------------------------------------------------------------
-spec start(map()) -> ok | {error, term()} | {timeout, [atom()]}.
%-------------------------------------------------------------
start(SS) ->
    mnesia:start(),
    mnesia:wait_for_tables(db_schemas:schema_names(SS), 10000).
 
stop() -> mnesia:stop().



%-------------------------------------------------------------
% Returns the number of records in the specified table. 
%-------------------------------------------------------------
-spec table_size(atom()) -> integer().
%-------------------------------------------------------------
table_size(Table) -> mnesia:table_info(Table, size).


%-------------------------------------------------------------
% Returns a list of tuples [{TableName, TableSize}]. 
%-------------------------------------------------------------
-spec table_sizes(map()) -> list().
%-------------------------------------------------------------
table_sizes(SS) -> table_sizes_next(db_schemas:schema_names(SS)).

table_sizes_next([]) -> [];
table_sizes_next(Tables) -> table_sizes_next(Tables, []).

table_sizes_next([], Sizes) -> lists:reverse(Sizes);
table_sizes_next([H|T], Sizes) -> table_sizes_next(T, [{H, table_size(H)} | Sizes]).


