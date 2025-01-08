-module(mb_db_management).
-include("../include/mb.hrl").

-export([deploy/1, table_sizes/1, table_size/2, setup_mnesia_schema/0, get_mnesia_dir/0]).


%-------------------------------------------------------------
% Installs the mnesia schema and then creates the tables define
% in the schema specifications. The table attributes need to be 
% completed in the schema specifications.
%
% Two variants:
%    deploy/1: installs mnesia schema on the local node only.
%    deploy/2: installs mnesia schema on the nodes provided in NodeList.
%
% TODO: 
%     - Make the deploy intelligent by extracting all the nodes
%       from SSG and then installing the schema on them.
%     - Validate the SSG before starting the deploy.
%     - If there is an mnesia schema installed, change its config
%       to support the expanded nodes list.
%-------------------------------------------------------------
-spec deploy(mb_ssg()) -> ok | mb_error().
%-------------------------------------------------------------
deploy(SSG) -> 
    case update_mnesia_schema(SSG) of 
        ok -> create_tables(SSG);
        {error, Reason} -> {error, Reason} 
    end.



%-------------------------------------------------------------
-spec create_tables(mb_ssg()) -> ok | mb_error().
%-------------------------------------------------------------
create_tables(SSG) ->
    
    SchemaNames = mb_ssg:schema_names(SSG),

    create_tables(SchemaNames, SSG).


create_tables([], _) -> ok;
create_tables([NextSchemaName | T], SSG) ->

    io:format("[mb::db_management::create_tables]: (...)~n"),
    io:format("[mb::db_management::create_tables]: schema type ~p~n", [mb_ssg:get_schema_attribute(?SCHEMA_TYPE, NextSchemaName,SSG)]),

    case mnesia:create_table(NextSchemaName, [{attributes, mb_ssg:field_names(NextSchemaName, SSG)},
                             {?SCHEMA_TYPE, mb_ssg:get_schema_attribute(?SCHEMA_TYPE, NextSchemaName,SSG)},
                             {?DISC_COPIES, mb_ssg:get_schema_attribute(?DISC_COPIES, NextSchemaName,SSG)},
                             {?DISC_ONLY_COPIES, mb_ssg:get_schema_attribute(?DISC_ONLY_COPIES, NextSchemaName, SSG)},
                             {?RAM_COPIES, mb_ssg:get_schema_attribute(?RAM_COPIES, NextSchemaName, SSG)}]) of

        {atomic, _} -> create_tables(T, SSG);
        {aborted, Reason} -> 
            io:format("failed to create table ~p~n", [Reason]),
            {aborted, Reason}
    end.

%-------------------------------------------------------------
% Returns the number of records in the specified table. 
%-------------------------------------------------------------
-spec table_size(atom(), map()) -> integer().
%-------------------------------------------------------------
table_size(SchemaName, SSG) -> 
    
    case mb_ssg:is_schema(SchemaName, SSG) of 
        true -> mnesia:table_info(SchemaName, size);
        false -> {error, {invalid_schema_name, SchemaName}}
    end.


%-------------------------------------------------------------
% Returns a list of tuples [{SchemaName, TableSize}]. 
%-------------------------------------------------------------
-spec table_sizes(mb_ssg()) -> list().
%-------------------------------------------------------------
table_sizes(SSG) -> table_sizes_next(mb_ssg:schema_names(SSG)).

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
    case mb_ssg:schema_names(SSG) of 
        {error, Reason} -> {error, Reason};
        SchemaNames -> get_storage_nodes(SchemaNames, [], SSG) 
    end.

get_storage_nodes([], Aggregate, _) -> lists:uniq(Aggregate);
get_storage_nodes([NextSchema | T], Aggregate, SSG) ->
    case mb_ssg:get_schema_attribute(?RAM_COPIES, NextSchema, SSG) of
        {error, Reason} -> {error, Reason};
        RamCopies ->
            case mb_ssg:get_schema_attribute(?DISC_COPIES, NextSchema, SSG) of
                {error, Reason} -> {error, Reason};
                DiscCopies ->
                    case mb_ssg:get_schema_attribute(?DISC_ONLY_COPIES, NextSchema, SSG) of
                        {error, Reason} -> {error, Reason};
                        DiscOnlyCopies ->
                            NodesList = lists:uniq(RamCopies ++ DiscCopies ++ DiscOnlyCopies),
                            get_storage_nodes(T, Aggregate ++ NodesList, SSG)
                    end 
            end
    end.


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
get_mnesia_dir() -> ?MNESIA_ROOT_DIR ++ "/" ++ atom_to_list(node()).


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
setup_mnesia_schema() -> 

    case mnesia:change_table_copy_type(schema, node(), ?DISC_COPIES) of 
        {atomic, _} -> 

            MnesiaSchemaNodes = mnesia:system_info(db_nodes),
            ClusterNodes = [node()] ++ nodes(),

            DeltaNodes = lists:subtract(ClusterNodes, MnesiaSchemaNodes),

            case DeltaNodes of 
                [] -> ok;
                _ -> 
                    case mnesia:change_config(extra_db_nodes, DeltaNodes) of
                        {ok, _} -> ok;
                        {error, Reason} -> {error, Reason}
                    end 
            end;

        {aborted, Reason} -> {error, Reason} 
    end.


%-------------------------------------------------------------
% Installs the mnesia schema on the specified nodes for all
% SSG tables. 
%-------------------------------------------------------------
-spec update_mnesia_schema(mb_ssg()) -> ok | mb_error().
%-------------------------------------------------------------
update_mnesia_schema(SSG) -> 
    
    io:format("[mb::db_management::update_mnesia_schema]: (...)~n"),

    case get_storage_nodes(SSG) of
        {error, Reason1} -> {error, Reason1};
        [] -> {error, storage_nodes_not_defined};
        Nodes ->
            % 1. Are the nodes part of the cluster?
            ClusterNodes = [node()] ++ nodes(),

            io:format("[mb::db_management::update_mnesia_schema]: ssg nodes ~p~n", [Nodes]),
            io:format("[mb::db_management::update_mnesia_schema]: cluster nodes ~p~n", [ClusterNodes]),

            case mb_utilities:is_subset(Nodes, ClusterNodes) of 
                true ->
                    % Excellent, we have a valid list of nodes
                    % Now we need to check if the current schema
                    % has these nodes included, otherwise, we need
                    % to do some schema configuraiton changes.
                    io:format("[mb::db_management::update_mnesia_schema]: ssg nodes subset of cluster nodes~n"),
                    MnesiaSchemaNodes = mnesia:system_info(db_nodes),

                    io:format("[mb::db_management::update_mnesia_schema]: mnesia schema nodes ~p~n", [MnesiaSchemaNodes]),

                    DeltaNodes = lists:subtract(Nodes, MnesiaSchemaNodes),

                    io:format("[mb::db_management::update_mnesia_schema]: delta nodes ~p~n", [DeltaNodes]),

                    case DeltaNodes of 
                        [] -> ok;
                        _ -> 
                            case mnesia:change_config(extra_db_nodes, DeltaNodes) of
                                {ok, _} -> 
                                    io:format("[mb::db_management::update_mnesia_schema]: mnesia change config ok ~n"),
                                    ok;

                                {error, Reason} -> {error, Reason}
                            end 
                    end;

                false -> 
                    % Maybe if we ping the nodes we could add them to the cluster.
                    io:format("[mb::db_management::update_mnesia_schema]: ssg nodes contain nodes not in cluster~n"),
                    case mb_utilities:ping_nodes(Nodes) of 
                        true -> 
                            io:format("[mb::db_management::update_mnesia_schema]: nodes pinged successfully~n"),
                            % Okay, added to the cluster, let us try the installation
                            % again.
                            update_mnesia_schema(SSG);
                        false -> 
                            io:format("[mb::db_management::update_mnesia_schema]: could not ping nodes~n"),
                            {error, {nodes_not_in_cluster, lists:subtract(Nodes, ClusterNodes)}}
                    end 
            end 
    end.