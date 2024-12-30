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
%       from SSG and then installing the schema on them.
%     - Validate the SSG before starting the install.
%     - If there is an mnesia schema installed, change its config
%       to support the expanded nodes list.
%-------------------------------------------------------------
-spec install(map()) -> ok | mb_error().
%-------------------------------------------------------------
install(SSG) -> 
    case get_storage_nodes(SSG) of
        {error, Reason1} -> {error, Reason1};
        [] -> {error, storage_nodes_not_defined};
        Nodes ->
            % 1. Is local node in the list? If not, add it.
            case lists:member(node(), Nodes) of 
                true -> UpdatedNodes = Nodes;
                false -> UpdatedNodes = [node() | Nodes] 
            end,

            % 2. Is mnesia schema created? If not, create one.
            CreateSchema = case mnesia:table_info(schema, storage_type) of
                undefined -> 
                    % Schema not created
                    case mnesia:create_schema([node()]) of
                        {error, Reason2} -> {error, Reason2};
                        _ -> ok
                    end;

                _ -> ok
            end,

            case CreateSchema of 
                ok ->
                    % 3. Add remaining nodes not already in the schema 
                    %    and start mnesia.
                    CurrentNodes = mnesia:system_info(db_nodes),

                    case lists:subtract(UpdatedNodes, CurrentNodes) of 
                        [] -> application:start(mnesia);
                        ExtraNodes ->
                            % TODO: this may be dangerous ...
                            case mnesia:change_config(extra_db_nodes, ExtraNodes) of
                                {ok, _} -> application:start(mnesia);
                                {error, Reason3} -> {error, Reason3}
                            end
                    end;

                _ -> CreateSchema
            end
    end.

%-------------------------------------------------------------
-spec install(list(), map()) -> ok | mb_error().
%-------------------------------------------------------------
install(NodeList, SSG) ->
    
    case mnesia:create_schema(NodeList) of
        ok -> ok;
        {error, Reason1} -> io:format("failed to create get_schema: ~w~n", [Reason1])
    end,

    mnesia:start(),
    
    SchemaNames = mb_schemas:schema_names(SSG),

    install_next(SchemaNames, SSG).


install_next([], _) -> ok;
install_next([NextSchemaName | T], SSG) ->

    case mnesia:create_table(NextSchemaName, [{attributes, mb_schemas:field_names(NextSchemaName, SSG)},
                             {type, mb_schemas:get_schema_attribute(type, NextSchemaName,SSG)},
                             {disc_copies, mb_schemas:get_schema_attribute(disc_copies, NextSchemaName,SSG)},
                             {disc_only_copies, mb_schemas:get_schema_attribute(disc_only_copies, NextSchemaName, SSG)},
                             {ram_copies, mb_schemas:get_schema_attribute(ram_copies, NextSchemaName, SSG)}]) of

        {atomic, _} -> install_next(T, SSG);
        {aborted, Reason2} -> io:format("failed to create table ~p~n", [Reason2])
    end.


%-------------------------------------------------------------
-spec start(map()) -> ok | {error, term()} | {timeout, [atom()]}.
%-------------------------------------------------------------
start(SSG) ->
    mnesia:start(),
    mnesia:wait_for_tables(mb_schemas:schema_names(SSG), 10000).
 
stop() -> mnesia:stop().



%-------------------------------------------------------------
% Returns the number of records in the specified table. 
%-------------------------------------------------------------
-spec table_size(atom(), map()) -> integer().
%-------------------------------------------------------------
table_size(SchemaName, SSG) -> 
    
    case mb_schemas:is_schema(SchemaName, SSG) of 
        true -> mnesia:table_info(SchemaName, size);
        false -> {error, {invalid_schema_name, SchemaName}}
    end.


%-------------------------------------------------------------
% Returns a list of tuples [{SchemaName, TableSize}]. 
%-------------------------------------------------------------
-spec table_sizes(mb_ssg()) -> list().
%-------------------------------------------------------------
table_sizes(SSG) -> table_sizes_next(mb_schemas:schema_names(SSG)).

table_sizes_next([]) -> [];
table_sizes_next(Tables) -> table_sizes_next(Tables, []).

table_sizes_next([], Sizes) -> lists:reverse(Sizes);
table_sizes_next([H|T], Sizes) -> table_sizes_next(T, [{H, mnesia:table_info(H, size)} | Sizes]).


%-------------------------------------------------------------
% Returns a list of tuples [{SchemaName, TableSize}]. 
%-------------------------------------------------------------
-spec get_storage_nodes(mb_ssg()) -> list() | mb_error().
%-------------------------------------------------------------
get_storage_nodes(SSG) ->
    case mb_schemas:schema_names(SSG) of 
        {error, Reason} -> {error, Reason};
        SchemaNames -> get_storage_nodes(SchemaNames, [], SSG) 
    end.

get_storage_nodes([], Aggregate, _) -> lists:uniq(Aggregate);
get_storage_nodes([NextSchema | T], Aggregate, SSG) ->
    case mb_schemas:get_schema_attribute(ram_copies, NextSchema, SSG) of
        {error, Reason} -> {error, Reason};
        RamCopies ->
            case mb_schemas:get_schema_attribute(disc_copies, NextSchema, SSG) of
                {error, Reason} -> {error, Reason};
                DiscCopies ->
                    case mb_schemas:get_schema_attribute(disc_only_copies, NextSchema, SSG) of
                        {error, Reason} -> {error, Reason};
                        DiscOnlyCopies ->
                            NodesList = RamCopies ++ DiscCopies ++ DiscOnlyCopies,
                            get_storage_nodes(T, Aggregate ++ NodesList, SSG)
                    end 
            end
    end.