-module(db_schemas).

-define(CURRENT_VERSION, "0.2").

-define(VERSION, version).
-define(INDEX, index).
-define(SCHEMAS, schemas).
-define(POSITION, position).
-define(DISC_COPIES, disc_copies).
-define(DISC_ONLY_COPIES, disc_only_copies).
-define(RAM_COPIES, ram_copies).

-define(FIELDS, fields).
-define(NAME, name).
-define(LABEL, label).
-define(ROLE, role).
-define(TYPE, type).
-define(DESCRIPTION, description).
-define(PRIORITY, priority).
-define(DEFAULT_VALUE, default_value).

-define(GEN_H_FILE_NAME, "test.hrl").
-define(QUERY_MODULE, db_query).
-define(MODIFY_MODULE, db_edit).
-define(MANAGE_DB_MODULE, db_management).

% Schema management APIs
-export([new/0, 
         add_schema/2, 
         delete_schema/2,
         is_schema_attribute/1,
         is_schema_attribute/2,
         is_schema/2, 
         schemas/1, 
         get_schema/2, 
         set_schema_attributes/3,
         set_schema_attribute/4,
         get_schema_attribute/3,
         schema_specifications/1, 
         schema_names/1]).

-export([generate/2, generate/4]).

% Field Management APIs
-export([add_field/3, 
         is_field_attribute/1,
         is_field_attribute/2,
         is_field/3, 
         get_field/3,
         fields/2, 
         field_count/2,
         mandatory_field_count/2,
         field_names/2,
         key_name/2, 
         key_type/2,
         field_position/3, 
         get_field_attribute/4,
         set_field_attributes/4, 
         set_field_attribute/5, 
         update_fields/3]).

% Utility APIs
-export([convert_from_stirng/2, 
         get_type/1, 
         compare_fields_from_specs/2,
         build_schema_record_from_specifications/2,
         convert_schema_data_avp_list_into_record_tuple/2]).





%============================================================
%    SCHEMA MANAGEMENT APIs
%============================================================

%-------------------------------------------------------------
% Function:
% Purpose:  
% Returns:  
%-------------------------------------------------------------
schema_specifications(Module) when is_atom(Module) -> Module:schema_specification().

%-------------------------------------------------------------
% Function:
% Purpose:  
% Returns:  
%-------------------------------------------------------------
new() -> #{?VERSION=>?CURRENT_VERSION,
           ?SCHEMAS=>[]}.

%-------------------------------------------------------------
% Function:
% Purpose:  Add a new get_schema to the specifications.
% Returns:  
%-------------------------------------------------------------
add_schema(SchemaName, SS) when is_map(SS) ->

    % Get the list of schemas and their specification maps.
    SchemasList = schemas(SS),

    % We will only proceed with the add if the
    % get_schema name does not exist.
    case lists:keymember(SchemaName, 1, SchemasList) of
        false ->
            % Create a default get_schema specification map for this get_schema.
            Schema = create_schema(SchemaName),

            % Update the schemas list, the map, and return the updated map.
            UpdatedSchemasList = SchemasList ++ [{SchemaName, Schema}],
            maps:update(?SCHEMAS, UpdatedSchemasList, SS);

        true -> {error, {schema_exists, SchemaName}}
    end;

add_schema(_, SS) -> {error, {invalid_argument, SS}}.

%-------------------------------------------------------------
% Function:
% Purpose:  Add a new get_schema to the specifications.
% Returns:  
%-------------------------------------------------------------
delete_schema(SchemaName, SS) when is_map(SS) ->

    SchemasList = schemas(SS),
    UpdatedSchemasList = lists:keydelete(SchemaName, 1, SchemasList),
    maps:update(?SCHEMAS, UpdatedSchemasList, SS);

delete_schema(_, SS) -> {error, {invalid_argument, SS}}.

%-------------------------------------------------------------
% Function:
% Purpose:  
% Returns:  
%-------------------------------------------------------------
get_schema(SchemaName, SS) when is_map(SS) ->

    case schemas(SS) of 
        {error, Reason} -> {error, Reason};
        
        Schemas ->
            case lists:keyfind(SchemaName, 1, Schemas) of 
                {SchemaName, Specifications} -> Specifications;
                false -> {error, {schema_name_not_found, SchemaName}}
            end
    end;

get_schema(_, SS) -> {error, {invalid_argument, SS}}.



%-------------------------------------------------------------
% Function:
% Purpose:  
% Returns:  
%-------------------------------------------------------------
schemas(SS) when is_map(SS) ->

    case maps:find(?SCHEMAS, SS) of 
        {ok, Schemas} -> Schemas; 
        error -> {error, invalid_specifications}
    end;

schemas(SS) -> {error, {invalid_argument, SS}}.


%-------------------------------------------------------------
% Function:
% Purpose:  
% Returns:  
%-------------------------------------------------------------
schema_names(SS) when is_map(SS) -> 
    SchemasList = schemas(SS),
    schema_names([], SchemasList);

schema_names(SS) -> {error, {invalid_argument, SS}}.

schema_names(NameList, []) -> lists:reverse(NameList);
schema_names(NameList, [{Name, _} |T]) ->
    schema_names([Name | NameList], T).


%-------------------------------------------------------------
% Function:
% Purpose:  
% Returns: 
%-------------------------------------------------------------
is_schema(SchemaName, SS) when is_map(SS) ->

    case maps:find(?SCHEMAS, SS) of 
        {ok, Schemas} -> lists:keymember(SchemaName, 1, Schemas);
        error -> {error, {invalid_schema_name, SchemaName}} 
    end;

is_schema(_, SS) -> {error, {invalid_argument, SS}}.


%-------------------------------------------------------------
%  
%-------------------------------------------------------------
is_schema_attribute(Attribute) -> 

   case Attribute of 
        type -> true;
        disc_copies -> true;
        disc_only_copies -> true;
        ram_copies -> true;
        fields -> true;
        _ -> false
    end.

%-------------------------------------------------------------
% Function:
% Purpose:  
% Returns:  boolean()
%-------------------------------------------------------------
is_schema_attribute(Attribute, Value) -> 

   case Attribute of 
        type -> 
            case Value of 
                set -> true;
                ordered_set -> true;
                bag -> true;
                _ -> false 
            end;

        disc_copies -> is_list(Value);
        disc_only_copies -> is_list(Value);
        ram_copies -> is_list(Value);
        fields -> is_field_list(Value);
        _ -> false
    end.

%-------------------------------------------------------------
% Function:
% Purpose:  Sets the schema attributes in batch but not fields.
% Returns:  
%-------------------------------------------------------------
set_schema_attributes([], _, SS) when is_map(SS) -> SS;

set_schema_attributes([{Attribute, Value} | T], SchemaName, SS) when is_map(SS) ->

    case (is_schema_attribute(Attribute, Value) and (Attribute /= fields)) of 
        true -> 
            case Attribute of 
                fields -> update_fields(Value, SchemaName, SS);
                _ ->
                    case get_schema(SchemaName, SS) of
                        {error, Reason} -> {error, Reason};
                        Schema -> 
                            UpdatedSchemaSpecifications = maps:update(Attribute, Value, Schema),
                            SchemasList = schemas(SS),
                            UpdatedSchemasList = lists:keyreplace(SchemaName, 1, SchemasList, {SchemaName, UpdatedSchemaSpecifications}),
                            UpdatedSS = maps:update(?SCHEMAS, UpdatedSchemasList, SS),
                            set_schema_attributes(T, SchemaName, UpdatedSS)
                    end
            end;

        false -> {error, {invalid_attribute, Attribute}}
    end;

set_schema_attributes(AvpList, SchemaName, SS) -> {error, {invalid_argument, {AvpList, SchemaName, SS}}}.

%-------------------------------------------------------------
% Function:
% Purpose: 
% Returns:  
%-------------------------------------------------------------
set_schema_attribute(Attribute, Value, SchemaName, SS) when is_map(SS) ->

    case is_schema_attribute(Attribute, Value) of 
        true ->
            case Attribute of 
                %% TODO: revisit why we are doing update_fields here.
                fields -> update_fields(Value, SchemaName, SS);
                _ ->
                    case get_schema(SchemaName, SS) of
                        {error, Reason} -> {error, Reason};
                        Schema ->
                            UpdatedSchemaSpecifications = maps:update(Attribute, Value, Schema),
                            SchemasList = schemas(SS),
                            UpdatedSchemasList = lists:keyreplace(SchemaName, 1, SchemasList, {SchemaName, UpdatedSchemaSpecifications}),
                            maps:update(?SCHEMAS, UpdatedSchemasList, SS)
                    end
            end;

        false -> {error, {invalid_attribute, Attribute}}
    end;

set_schema_attribute(_, _, _, SS) -> {error, {invalid_argument, SS}}.


%-------------------------------------------------------------
% Function:
% Purpose:  
% Returns:  
%------------------------------------------------------------- 
get_schema_attribute(Attribute, SchemaName, SS) when is_map(SS) -> 
    % io:format("~nattribute: ~p~n", [Attribute]),

    case is_schema_attribute(Attribute) of
        true ->
            case get_schema(SchemaName, SS) of 
                {error, Reason} -> {error, Reason};
                Schema -> maps:get(Attribute, Schema)
            end;
        false -> {error, {invalid_attribute, Attribute}}
    end;

get_schema_attribute(_, _, SS) -> {error, {invalid_argument, SS}}.



%============================================================
%    FIELD MANAGEMENT APIs
%============================================================

%-------------------------------------------------------------
% Function:
% Purpose:  
% Returns:  
%-------------------------------------------------------------
add_field(FieldName, SchemaName, SS) when is_map(SS) ->

    Schemas = schemas(SS),

    case lists:keyfind(SchemaName, 1, Schemas) of 
        {SchemaName, SchemaSpecifications} ->
            FieldList = maps:get(?FIELDS, SchemaSpecifications),

            case add_field(FieldName, FieldList) of 
                {error, Reason} -> {error, Reason};

                UpdatedFieldList ->
                    UpdatedSchemaSpecifications = maps:update(?FIELDS, UpdatedFieldList, SchemaSpecifications),
                    UpdatedSchemas = lists:keyreplace(SchemaName, 1, Schemas, {SchemaName, UpdatedSchemaSpecifications}),
                    maps:update(?SCHEMAS, UpdatedSchemas, SS)
            end;

        false -> {error, {schema_name_not_found, SchemaName}}
    end;

add_field(FieldName, SchemaName, SS) -> {error, {invalid_argument, {FieldName, SchemaName, SS}}}.


%-------------------------------------------------------------
% Function:
% Purpose:  
% Returns: 
%-------------------------------------------------------------
get_field(FieldName, SchemaName, SS) when is_map(SS) ->
    
    case fields(SchemaName, SS) of 
        {error, Reason} -> {error, Reason};
        FieldsList ->
            case lists:keyfind(FieldName, 1, FieldsList) of
                false -> {error, {not_found, FieldName}};
                FieldSpec -> FieldSpec 
            end
    end;

get_field(_, _, SS) -> {error, {invalid_argument, SS}}.

%-------------------------------------------------------------
% Function:
% Purpose:  
% Returns:  [{FieldName, FieldSpecifications}]
%-------------------------------------------------------------
fields(SchemaName, SS) when is_map(SS) ->

    Schemas = schemas(SS),
    
    case lists:keyfind(SchemaName, 1, Schemas) of 
        {SchemaName, SchemaSpecifications} ->
            {ok, Fields} = maps:find(?FIELDS, SchemaSpecifications),
            Fields;
        false -> {error, {schema_name_not_found, SchemaName}}
    end;

fields(_, SS) -> {error, {invalid_argument, SS}}.


%-------------------------------------------------------------
%-------------------------------------------------------------
is_field_attribute(Attribute) ->

    case Attribute of 
        ?LABEL -> true;
        ?ROLE -> true;
        ?TYPE -> true;
        ?PRIORITY -> true;
        ?DEFAULT_VALUE -> true;
        ?DESCRIPTION -> true;
        _ -> false 
    end.

%-------------------------------------------------------------
%-------------------------------------------------------------
is_field_attribute(Attribute, Value) ->

    case Attribute of
        ?LABEL -> is_list(Value);
        ?ROLE -> 
            case Value of
                field -> true;
                key -> true;
                _ -> false
            end;

        ?TYPE -> is_type(Value);

        ?PRIORITY -> 
            case Value of
                mandatory -> true;
                optional -> true;
                _ -> false
            end;

        ?DEFAULT_VALUE -> true;
        ?DESCRIPTION -> is_list(Value)
    end.

%-------------------------------------------------------------
% Function:
% Purpose:  
% Returns:  
%-------------------------------------------------------------
set_field_attribute(Attribute, Value, FieldName, SchemaName, SS) when is_map(SS) ->

    % Screen the attribute/value pair.
    case is_field_attribute(Attribute, Value) of
        true ->
            Schemas = schemas(SS),
            
            case lists:keyfind(SchemaName, 1, Schemas) of 
                {SchemaName, SchemaSpecifications} -> 
                    FieldList = maps:get(?FIELDS, SchemaSpecifications),

                    UpdatedFieldList = update_field(Attribute, Value, FieldName, FieldList),

                    UpdatedSchemaSpecifications = maps:update(?FIELDS, UpdatedFieldList, SchemaSpecifications),
                    UpdatedSchemas = lists:keyreplace(SchemaName, 1, Schemas, {SchemaName, UpdatedSchemaSpecifications}),
                    maps:update(?SCHEMAS, UpdatedSchemas, SS);

                false -> {error, {schema_name_not_found, SchemaName}}

            end;

        false -> {error, {invalid_field_attribute, Attribute}}
    end;

set_field_attribute(_, _, _, _, SS) -> {error, {invalid_argument, SS}}.


%-------------------------------------------------------------
% Function:
% Purpose:  
% Returns:  
%-------------------------------------------------------------
set_field_attributes(AvpList, FieldName, SchemaName, SS) when is_map(SS) ->

    % 1. Get the schemas list
    % 2. Extract the specifications for SchemaName
    % 3. Extract the field list from the get_schema specifications
    % 4. Find FieldName and get its field specifications
    % 5. Cycle through all the attributes and update them
    % 6. Now we have a new field specifications, put it back in the field list
    % 7. Put the field list back in the get_schema specifications
    % 8. Update the get_schema specifications map

    Schemas = schemas(SS),

    case lists:keyfind(SchemaName, 1, Schemas) of

        {SchemaName, SchemaSpecifications} -> 
            FieldList = maps:get(?FIELDS, SchemaSpecifications),

            case lists:keyfind(FieldName, 1, FieldList) of 
                {FieldName, FieldSpecifications} -> 
                
                    case set_field_attributes(AvpList, FieldSpecifications) of 
                        {error, Reason} -> {error, Reason};

                        UpdatedFieldSpecifications ->
                            UpdatedFieldList = lists:keyreplace(FieldName, 1, FieldList, {FieldName, UpdatedFieldSpecifications}),
                            UpdatedSchemaSpecifications = maps:update(?FIELDS, UpdatedFieldList, SchemaSpecifications),
                            UpdatedSchemas = lists:keyreplace(SchemaName, 1, Schemas, {SchemaName, UpdatedSchemaSpecifications}),
                            maps:update(?SCHEMAS, UpdatedSchemas, SS)
                    end;
                
                false -> {error, {field_name_not_found, FieldName}}
            end;

        false -> {error, {schema_name_not_found, SchemaName}}
    end;

set_field_attributes(_, _, _, SS) -> {error, {invalid_argument, SS}}.


%-------------------------------------------------------------
% Function:
% Purpose:  
% Returns:  
%-------------------------------------------------------------
get_field_attribute(Attribute, FieldName, SchemaName, SS) when is_map(SS) -> 

    case fields(SchemaName, SS) of 
        {error, Reason} -> {error, Reason};
        
        Fields ->
            case lists:keyfind(FieldName, 1, Fields) of 
                {FieldName, FieldSpecifications} -> 
                    case is_field_attribute(Attribute) of 
                        true -> maps:get(Attribute, FieldSpecifications);
                        false -> {error, {invalid_argument, Attribute}}
                    end;

                false -> {error, {field_name_not_found, FieldName}}
            end
    end;

get_field_attribute(_, _, _, SS) -> {error, {invalid_argument, SS}}.


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
is_field(FieldName, SchemaName, SS) when is_map(SS) ->

    Fields = fields(SchemaName, SS),
    lists:keymember(FieldName, 1, Fields);

is_field(_, _, SS) -> {error, {invalid_argument, SS}}.


%-------------------------------------------------------------
% Function:
% Purpose:  
% Returns:  
%------------------------------------------------------------- 
update_fields(FieldList, SchemaName, SS) when is_map(SS) -> 

    %% First, validate the field list
    case is_field_list(FieldList) of 
        true -> 
            SchemasList = schemas(SS),

            case lists:keyfind(SchemaName, 1, SchemasList) of

                {SchemaName, SchemaSpecifications} ->
                    UpdatedSchemaSpecifications = maps:update(?FIELDS, FieldList, SchemaSpecifications),

                    UpdatedSchemasList = lists:keyreplace(SchemaName, 1, SchemasList, {SchemaName, UpdatedSchemaSpecifications}),
                    maps:update(?SCHEMAS, UpdatedSchemasList, SS);

                false -> {error, {schema_name_not_found, SchemaName}}
            end;

        false -> {error, {bad_field_list, FieldList}}
    end;

update_fields(_, _, SS) -> {error, {invalid_argument, SS}}.

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
field_count(SchemaName, SS) when is_map(SS) ->

    case fields(SchemaName, SS) of 
        {error, Reason} -> {error, Reason};
        Fields -> length(Fields) 
    end;

field_count(_, SS) -> {error, {invalid_argument, SS}}.


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
mandatory_field_count(SchemaName, SS) when is_map(SS) ->

    case fields(SchemaName, SS) of 
        {error, Reason} -> {error, Reason};
        Fields -> mandatory_field_count_next(Fields, 0)
    end;

mandatory_field_count(_, SS) -> {error, {invalid_argument, SS}}.


mandatory_field_count_next([], FinalMandatoryCount) -> FinalMandatoryCount;
mandatory_field_count_next([{_, FieldSpec} | T], MandatoryCount) ->
    
    case maps:get(priority, FieldSpec) of 
        mandatory -> mandatory_field_count_next(T, MandatoryCount+1);
        optional -> mandatory_field_count_next(T, MandatoryCount) 
    end.

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
field_names(SchemaName, SS) when is_map(SS) -> 
    case fields(SchemaName, SS) of 
        {error, Reason} -> {error, Reason};
        Fields -> field_names_next(Fields, [])
    end;

field_names(_, SS) -> {error, {invalid_argument, SS}}.


field_names_next([], FinalList) -> lists:reverse(FinalList);
field_names_next([{FieldName, _} | T], CompiledList) -> field_names_next(T, [FieldName | CompiledList]).


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
key_name(SchemaName, SS) when is_map(SS) -> 

    case fields(SchemaName, SS) of 
        {error, Reason} -> {error, Reason};
         [Key|_] -> Key
    end;

key_name(_, SS) -> {error, {invalid_argument, SS}}.


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
key_type(SchemaName, SS) when is_map(SS) -> 

    case field_names(SchemaName, SS) of 
        {error, Reason} -> {error, Reason};
         [Key|_] -> get_field_attribute(type, Key, SchemaName, SS)
    end;

key_type(_, SS) -> {error, {invalid_argument, SS}}.

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
field_position(FieldName, SchemaName, SS) -> get_field_attribute(?POSITION, FieldName, SchemaName, SS).

%============================================================
%    CODE GENERATION APIs
%============================================================

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
generate(Module, SS) -> generate(Module, ".", ".", SS).

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns:  
%-------------------------------------------------------------
generate(Module, SrcPath, HrlPath, SS) when (is_atom(Module) and is_list(SrcPath) and is_list(HrlPath) and is_map(SS)) ->

    io:format("generate::started~n"),

    case utilities:is_unquoted_atom(Module) of 

        
        true ->
            io:format("generate::is_unquoted_atom: true~n"),
            
            case file:open(HrlPath ++ "/" ++ atom_to_list(Module) ++ ".hrl", [write]) of 
                {ok, HrlIoDevice} ->

                    io:format("generate::header file opened for writing~n"),
                    
                    generate_records(SS, HrlIoDevice),

                    case file:open(SrcPath ++ "/" ++ atom_to_list(Module) ++ ".erl", [write]) of
                        {ok, SrcIoDevice}  -> 
                            
                            io:format("generate::source file opened for writing~n"),
                            
                            io:format(SrcIoDevice, "-module(~p).~n", [Module]),

                            

                            io:format(SrcIoDevice, "~n", []),
                            io:format(SrcIoDevice, "-export([schema_specifications/0]).~n", []),

                            io:format(SrcIoDevice, "-export([install/0, install/1, start/0, stop/0, table_size/1, table_sizes/0]).~n", []),
                            io:format(SrcIoDevice, "-export([schema_names/0, is_schema/1, is_field/2, schemas/0, get_schema/1, get_schema_attribute/2]).~n", []),
                            io:format(SrcIoDevice, "-export([fields/1, field_count/1, mandatory_field_count/1, field_names/1, key_name/1, key_type/1, field_position/2, get_field_attribute/3]).~n", []),
                            io:format(SrcIoDevice, "-export([read/2, select/4, select_or/6, select_and/6, build_matchhead/1]).~n", []),
                            io:format(SrcIoDevice, "-export([add/1, add/2, add/3, delete/1, delete/2, clear_all_tables/0]).~n", []),
                            io:format(SrcIoDevice, "-export([build_schema_record_from_specifications/1, convert_schema_data_avp_list_into_record_tuple/1]).~n", []),
                            io:format(SrcIoDevice, "~n", []),
                            io:format(SrcIoDevice, "schema_specifications() ->~n", []),
                            io:format(SrcIoDevice, "    ~p.~n", [SS]),    

                            io:format(SrcIoDevice, "~n~n", []),
                            io:format(SrcIoDevice, "%-------------------------------------------------------~n",[]),
                            io:format(SrcIoDevice, "%                DB Management Functions~n",[]),
                            io:format(SrcIoDevice, "%-------------------------------------------------------~n",[]),

                            generate_spec_function(install, ?MANAGE_DB_MODULE, SrcIoDevice),
                            generate_spec_function(install, "NodeList", ?MANAGE_DB_MODULE, SrcIoDevice),
                            generate_spec_function(start, ?MANAGE_DB_MODULE, SrcIoDevice),
                            generate_function(stop, ?MANAGE_DB_MODULE, SrcIoDevice),

                            generate_spec_function(table_size, "SchemaName", ?MANAGE_DB_MODULE, SrcIoDevice),
                            generate_spec_function(table_sizes, ?MANAGE_DB_MODULE, SrcIoDevice),

                            io:format(SrcIoDevice, "~n~n", []),
                            io:format(SrcIoDevice, "%-------------------------------------------------------~n",[]),
                            io:format(SrcIoDevice, "%                     Schema Functions~n",[]),
                            io:format(SrcIoDevice, "%-------------------------------------------------------~n",[]),

                            generate_spec_function(schema_names, ?MODULE, SrcIoDevice),
                            generate_spec_function(is_schema, "SchemaName", ?MODULE, SrcIoDevice),
                            generate_spec_function(is_field, "FieldName", "SchemaName", ?MODULE, SrcIoDevice),     
                            generate_spec_function(schemas, ?MODULE, SrcIoDevice),
                            generate_spec_function(get_schema, "SchemaName", ?MODULE, SrcIoDevice),
                            generate_spec_function(get_schema_attribute, "Attribute", "SchemaName", ?MODULE, SrcIoDevice),
                            generate_spec_function(fields, "SchemaName", ?MODULE, SrcIoDevice),
                            generate_spec_function(field_count, "SchemaName", ?MODULE, SrcIoDevice),
                            generate_spec_function(mandatory_field_count, "SchemaName", ?MODULE, SrcIoDevice),
                            generate_spec_function(field_names, "SchemaName", ?MODULE, SrcIoDevice),
                            generate_spec_function(key_name, "SchemaName", ?MODULE, SrcIoDevice),
                            generate_spec_function(key_type, "SchemaName", ?MODULE, SrcIoDevice),
                            generate_spec_function(field_position, "FieldName", "SchemaName", ?MODULE, SrcIoDevice),
                            generate_spec_function(get_field_attribute, "Attribute", "FieldName", "SchemaName", ?MODULE, SrcIoDevice),

                            io:format(SrcIoDevice, "~n~n", []),
                            io:format(SrcIoDevice, "%-------------------------------------------------------~n",[]),
                            io:format(SrcIoDevice, "%                     Query Functions~n",[]),
                            io:format(SrcIoDevice, "%-------------------------------------------------------~n",[]),

                            generate_function(read, "SchemaName", "Key", ?QUERY_MODULE, SrcIoDevice),

                            generate_spec_function(select, "SchemaName", "Field", "Oper", "Value", ?QUERY_MODULE, SrcIoDevice),
                            generate_spec_function(select_or, "SchemaName", "Field", "Oper1", "Value1", "Oper2", "Value2", ?QUERY_MODULE, SrcIoDevice),
                            generate_spec_function(select_and, "SchemaName", "Field", "Oper1", "Value1", "Oper2", "Value2", ?QUERY_MODULE, SrcIoDevice),
                            generate_spec_function(build_matchhead, "SchemaName", ?QUERY_MODULE, SrcIoDevice),

                            io:format(SrcIoDevice, "~n~n", []),
                            io:format(SrcIoDevice, "%-------------------------------------------------------~n",[]),
                            io:format(SrcIoDevice, "%                     Modify Functions~n",[]),
                            io:format(SrcIoDevice, "%-------------------------------------------------------~n",[]),

                            generate_spec_function(add, "Record", ?MODIFY_MODULE, SrcIoDevice),
                            generate_spec_function(add, "SchemaName", "Record", ?MODIFY_MODULE, SrcIoDevice),
                            generate_spec_function(add, "SchemaName", "Key", "Data", ?MODIFY_MODULE, SrcIoDevice),

                            generate_spec_function(delete, "TableKey", ?MODIFY_MODULE, SrcIoDevice),
                            generate_spec_function(delete, "SchemaName", "Key", ?MODIFY_MODULE, SrcIoDevice),

                            generate_spec_function(clear_all_tables, ?MODIFY_MODULE, SrcIoDevice),

                            io:format(SrcIoDevice, "~n~n", []),
                            io:format(SrcIoDevice, "%-------------------------------------------------------~n",[]),
                            io:format(SrcIoDevice, "%                     Utility Functions~n",[]),
                            io:format(SrcIoDevice, "%-------------------------------------------------------~n",[]),

                            generate_spec_function(build_schema_record_from_specifications, "SchemaName", ?MODULE, SrcIoDevice),
                            generate_spec_function(convert_schema_data_avp_list_into_record_tuple, "AvpList", ?MODULE, SrcIoDevice),
                            
                            file:close(SrcIoDevice);

                        {error, Reason} -> {error, Reason}
                    end;

                {error, Reason} -> {error, Reason}
            end;

        false -> {error, {invalid_module_name, Module}}
    end;

generate(Module, SrcPath, HrlPath, SS) -> 
    io:format("db_schemas::error, match guard failed~n"),
    {error, {invalid_argument, {Module, SrcPath, HrlPath, SS}}}.


%============================================================
%    UTILITY APIs
%============================================================

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: converts the value or crashes
%-------------------------------------------------------------
convert_from_stirng(Value, Type) ->

  case is_list(Value) of
    true ->
      case Type of
        integer -> {integer, utilities:string_to_integer(Value)};
        float -> {float, utilities:string_to_float(Value)};
        string -> {string, Value};
        list -> {list, Value};
        atom -> {atom, list_to_atom(Value)};
        tuple -> 
            case utilities:string_to_tuple(Value) of 
                {ok, Tuple} -> {tuple, Tuple};
                {error, Reason} -> {error, Reason}
            end;

        term -> {term, Value};
        _ -> {error, {unknown_type, Type}}
      end;

    false -> {error, {bad_input, Value}}
  end.


%-------------------------------------------------------------
% Function: 
% Purpose: Validates [{schema, SchemaName}, {<FieldName, FieldValue}, {<FieldName, FieldValue}, ...]
%          and converts it to a record tuple {SchemaName, FieldValu1, FieldValue2, ...}.
%          Fields need not be in the order listed in the schema specifications.
%          The mandatory fields must be listed for validation to pass.
%          Missing optional fields are augmented with default values.
%
% Returns: {SchemaName, FieldValue1, FieldValue2, ...} or {error, Reason}
%-------------------------------------------------------------
convert_schema_data_avp_list_into_record_tuple(AvpList, SS) when (is_list(AvpList) and is_map(SS)) -> 
    
    % io:format("~nAvp list: ~p~n", [AvpList]),
    
    case lists:keyfind(schema, 1, AvpList) of
        {schema, SchemaName} -> 
            % Get the ordered list of fields, [{FieldName, FieldSpecMap}]
            case fields(SchemaName, SS) of 
                {error, Reason} -> {error, Reason};
                Fields ->
                    case validate_schema_data_avp_list_next(Fields, lists:keydelete(schema, 1, AvpList), [{schema, SchemaName}]) of
                        {error, Reason} -> {error, Reason};
                        ValidatedList -> schema_data_avp_list_to_record_tuple(ValidatedList) 
                    end
            end;

        false -> {error, schema_name_not_in_avp}
    end;

convert_schema_data_avp_list_into_record_tuple(AvpList, SS) -> {error, {invalid_argument, {AvpList, SS}}}.


validate_schema_data_avp_list_next([], [], FinalList) -> FinalList;
validate_schema_data_avp_list_next([],_, _) -> {error, invalid_fields};
validate_schema_data_avp_list_next([{FieldName, FieldSpecMap} | F], AvpList, InterimList) ->

    case lists:keyfind(FieldName, 1, AvpList) of 

        {FieldName, FieldValue} -> 
            % Found it, the field is in the supplied list. Validate that
            % the supplied value has the right type.
            ExpectedType = maps:get(type, FieldSpecMap),
            ActualType = get_type(FieldValue),

            case ExpectedType == ActualType of
                true -> validate_schema_data_avp_list_next(F, lists:keydelete(FieldName, 1, AvpList), [{FieldName, FieldValue} | InterimList]);
                false -> {error, {invalid_type, {FieldName, FieldValue, ActualType}}}
            end;

        false ->
            % If mandatory, then validation fails, otherwise
            % we will select the default value.

            Priority = maps:get(priority, FieldSpecMap),

            case Priority of 
                mandatory -> {error, {missing_mandatory_field, FieldName}};
                optional -> validate_schema_data_avp_list_next(F, lists:keydelete(FieldName, 1, AvpList), [{FieldName, maps:get(default_value, FieldSpecMap)} | InterimList])
            end 
    end.

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns:  
%-------------------------------------------------------------
schema_data_avp_list_to_record_tuple(SchemaDataAvpList) -> 
    %io:format("~nconverting to tuple: ~p~n", [SchemaDataAvpList]),
    schema_data_avp_list_to_record_tuple_next(SchemaDataAvpList, []).

schema_data_avp_list_to_record_tuple_next([], FinalList) -> list_to_tuple(FinalList);
schema_data_avp_list_to_record_tuple_next([{_, Value} | T], InterimList) -> schema_data_avp_list_to_record_tuple_next(T, [Value | InterimList]);
schema_data_avp_list_to_record_tuple_next(InvalidList, _) -> {error, {invalid_argument, InvalidList}}.
%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns:  
%-------------------------------------------------------------
build_schema_record_from_specifications(SchemaName, SS) when is_map(SS) -> 
    tuple_to_list([SchemaName, field_names(SchemaName, SS)]);

build_schema_record_from_specifications(_, SS) -> {error, {invalid_argument, SS}}.

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns:  
%-------------------------------------------------------------
compare_fields_from_specs([], []) -> true;
compare_fields_from_specs(_, []) -> false;
compare_fields_from_specs([], _) -> false;
compare_fields_from_specs([Next1 | T1], [Next2 | T2]) ->

    {FieldName1, FieldSpec1} = Next1,
    {FieldName2, FieldSpec2} = Next2,

    case ((FieldName1 == FieldName2) and (FieldSpec1 == FieldSpec2)) of 
        true -> compare_fields_from_specs(T1, T2);
        false -> false 
    end.


%============================================================
%    PRIVATE FUNCTIONS
%============================================================


%-------------------------------------------------------------
% Function:
% Purpose:  
% Returns:  
%-------------------------------------------------------------
create_schema(SchemaName) when is_atom(SchemaName) -> 

    maps:put(?FIELDS, [], 
        maps:put(ram_copies, [], 
            maps:put(disc_only_copies, [], 
                maps:put(disc_copies, [], 
                    maps:put(type, set, 
                        maps:put(?NAME, SchemaName, maps:new()))))));

create_schema(SchemaName) -> {error, {invalid_argument, SchemaName}}.

%-------------------------------------------------------------
% Function:
% Purpose:  
% Returns:  
%-------------------------------------------------------------
create_field(FieldName) when is_atom(FieldName) ->

  maps:put(?DESCRIPTION, "", 
    maps:put(?DEFAULT_VALUE, not_defined, 
        maps:put(?PRIORITY, mandatory, 
            maps:put(?POSITION, 0, 
                maps:put(?TYPE, term, 
                    maps:put(?ROLE, field, 
                        maps:put(?LABEL, "", 
                            maps:put(?NAME, FieldName, maps:new()))))))));

create_field(SchemaName) -> {error, {invalid_argument, SchemaName}}.

%-------------------------------------------------------------
% Function:
% Purpose:  
% Returns:  
%-------------------------------------------------------------
add_field(FieldName, FieldList) when (is_atom(FieldName) and is_list(FieldList)) ->

    % We are building a field list for a get_schema.
    % We will create a default field specifications
    % and add it to the list. But first, make sure
    % the field name does not exist.

    case lists:keymember(FieldName, 1, FieldList) of 
        false -> 

            Field1 = create_field(FieldName),

            case length(FieldList) of 
                0 -> Field2 = maps:update(?ROLE, key, Field1);
                _ -> Field2 = maps:update(?ROLE, field, Field1) 
            end,

            Field3 = maps:update(?POSITION, length(FieldList)+1, Field2),
            FieldList ++ [{FieldName, Field3}];

        true -> {error, {field_exists, FieldName}}
    end;

add_field(FieldName, FieldList) -> {error, {invalid_argument, {FieldName, FieldList}}}.


%-------------------------------------------------------------
% Function:
% Purpose:  Sets field attributes except for position and role,
%           they are unchangeable.
% Returns:  
%-------------------------------------------------------------
update_field(Attribute, Value, FieldName, FieldList) when is_list(FieldList) ->

    case is_field_attribute(Attribute, Value) of 
        true -> 
            case Attribute of 
                ?POSITION -> {error, {attribute_already_set, Attribute}};
                ?ROLE -> {error, {attribute_already_set, Attribute}};
                _ ->
                    case lists:keyfind(FieldName, 1, FieldList) of 
                        
                        {FieldName, FieldSpecifications} ->
                            UpdatedFieldSpecifications = maps:update(Attribute, Value, FieldSpecifications),
                            lists:keyreplace(FieldName, 1, FieldList, {FieldName, UpdatedFieldSpecifications});

                        false -> {error, {field_name_not_found, {FieldName, FieldList}}}
                    end
            end;

        false -> {error, {invalid_attribute, {Attribute, Value}}}
    end;

update_field(_, _, _, FieldList) -> {error, {invalid_argument, FieldList}}.

      
%-------------------------------------------------------------
% Function:
% Purpose:  
% Returns:  
%------------------------------------------------------------- 
is_field_list([]) -> true;
is_field_list([{FieldName, FieldSpecifications} | T]) when is_map(FieldSpecifications) ->

    case maps:find(?NAME, FieldSpecifications) of 
        FieldName -> is_field_list(T);
        _ -> false 
    end; 
is_field_list(_) -> false.

%-------------------------------------------------------------
% Function:
% Purpose:  
% Returns:  
%-------------------------------------------------------------
set_field_attributes([], FieldSpecifications) -> FieldSpecifications;

set_field_attributes([{Attribute, Value} | T], FieldSpecifications) -> 
    case is_field_attribute(Attribute, Value) of 
        true -> set_field_attributes(T, maps:update(Attribute, Value, FieldSpecifications));
        false -> {error, {invalid_argument, {Attribute, Value}}}
    end.


%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns:  
%-------------------------------------------------------------
generate_function(FunctionName, BaseModule, IoDevice) ->
    io:format(IoDevice, "~n~p() -> ~p:~p().~n", 
        [FunctionName, BaseModule, FunctionName]).

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns:  
%-------------------------------------------------------------
%generate_function(FunctionName, Arg1, BaseModule, IoDevice) ->
%    io:format(IoDevice, "~n~p(~s) -> ~p:~p(~s).~n", 
%        [FunctionName, Arg1, BaseModule, FunctionName, Arg1]).

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns:  
%-------------------------------------------------------------
generate_function(FunctionName, Arg1, Arg2, BaseModule, IoDevice) ->
    io:format(IoDevice, "~n~p(~s, ~s) -> ~p:~p(~s, ~s).~n", 
        [FunctionName, Arg1, Arg2, BaseModule, FunctionName, Arg1, Arg2]).

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns:  
%-------------------------------------------------------------
%generate_function(FunctionName, Arg1, Arg2, Arg3, BaseModule, IoDevice) ->
%    io:format(IoDevice, "~n~p(~s, ~s, ~s) -> ~p:~p(~s, ~s, ~s).~n", 
%        [FunctionName, Arg1, Arg2, Arg3, BaseModule, FunctionName, Arg1, Arg2, Arg3]).

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns:  
%-------------------------------------------------------------
generate_spec_function(FunctionName, BaseModule, IoDevice) -> 
    io:format(IoDevice, "~n~p() -> ~p:~p(schema_specifications()).~n", 
        [FunctionName, BaseModule, FunctionName]).


%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns:  
%-------------------------------------------------------------
generate_spec_function(FunctionName, Arg1, BaseModule, IoDevice) -> 
    io:format(IoDevice, "~n~p(~s) -> ~p:~p(~s, schema_specifications()).~n", 
        [FunctionName, Arg1, BaseModule, FunctionName, Arg1]).

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns:  
%-------------------------------------------------------------
generate_spec_function(FunctionName, Arg1, Arg2, BaseModule, IoDevice) -> 
    io:format(IoDevice, "~n~p(~s, ~s) -> ~p:~p(~s, ~s, schema_specifications()).~n", 
        [FunctionName, Arg1, Arg2, BaseModule, FunctionName, Arg1, Arg2]).


%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns:  
%-------------------------------------------------------------
generate_spec_function(FunctionName, Arg1, Arg2, Arg3, BaseModule, IoDevice) -> 
    io:format(IoDevice, "~n~p(~s, ~s, ~s) -> ~p:~p(~s, ~s, ~s, schema_specifications()).~n", 
        [FunctionName, Arg1, Arg2, Arg3, BaseModule, FunctionName, Arg1, Arg2, Arg3]).


%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns:  
%-------------------------------------------------------------
generate_spec_function(FunctionName, Arg1, Arg2, Arg3, Arg4, BaseModule, IoDevice) -> 
    io:format(IoDevice, "~n~p(~s, ~s, ~s, ~s) -> ~p:~p(~s, ~s, ~s, ~s, schema_specifications()).~n", 
        [FunctionName, Arg1, Arg2, Arg3, Arg4, BaseModule, FunctionName, Arg1, Arg2, Arg3, Arg4]).


%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns:  
%-------------------------------------------------------------
generate_spec_function(FunctionName, Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, BaseModule, IoDevice) -> 
    io:format(IoDevice, "~n~p(~s, ~s, ~s, ~s, ~s, ~s) -> ~p:~p(~s, ~s, ~s, ~s, ~s, ~s, schema_specifications()).~n", 
        [FunctionName, Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, 
         BaseModule, FunctionName, Arg1, Arg2, Arg3, Arg4, Arg5, Arg6]).


%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns:  
%-------------------------------------------------------------
generate_records(SS, IoDevice) -> 

    Schemas = maps:get(?SCHEMAS, SS),
    generate_record(Schemas, IoDevice).


%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns:  
%-------------------------------------------------------------
generate_record([], _) -> ok;
generate_record([{SchemaName, SchemaSpecifications} | T], HrlIoDevice) -> 

    FieldList = maps:get(?FIELDS, SchemaSpecifications),
    io:format(HrlIoDevice, "-record(~p, {", [SchemaName]),
    generate_record_field(FieldList, HrlIoDevice),
    io:format(HrlIoDevice, "}).~n", []),

    generate_record(T, HrlIoDevice).


%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns:  
%-------------------------------------------------------------
generate_record_field([], _) -> ok;
generate_record_field([{FieldName, _FieldSpecifications} | T], HrlIoDevice) ->

    io:format(HrlIoDevice, "~p", [FieldName]),
    case length(T) > 0 of 
        true -> io:format(HrlIoDevice, ",", []);
        false -> ok 
    end,

    generate_record_field(T, HrlIoDevice).



%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns:  
%-------------------------------------------------------------
is_type(Type) ->

  case Type of
    integer -> true;
    float -> true;
    string -> true;
    list -> true;
    atom -> true;
    tuple -> true;
    term -> true;
    _ -> false 
  end.

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns:  
%-------------------------------------------------------------
get_type(Value) ->

  case is_integer(Value) of
    true -> integer;
    false ->

      case is_float(Value) of
        true -> float;
        false ->

          case is_list(Value) of
            true ->

              case utilities:is_printable_string(Value) of
                true -> string;
                false -> list
              end;

            false -> 

              case is_atom(Value) of
                true -> atom;
                false -> 

                  case is_tuple(Value) of 
                    true -> tuple;
                    false -> term
                  end
              end 
          end 
      end 
  end.

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns:  
%-------------------------------------------------------------
%validate_type(_Table, _FieldName, Value) ->

%  Type = ok, %get_field_type(Table, FieldName),
%  case Type of
%    integer -> is_integer(Value);
%    float -> is_float(Value);
%    string -> is_list(Value);
%    list -> is_list();
%    atom -> is_atom(Value);
%    tuple -> is_tuple(Value);
%    term -> true
%  end.




%============================================================
%    TEST and PROTOTYPE FUNCTIONS
%============================================================

%-------------------------------------------------------------
% Function: This is a test get_schema specifications. The registered
%           module must provide its own specifications.
% Purpose:  
% Returns:  
%
% 
%-------------------------------------------------------------
%schema_specifications() ->

%    #{
%        ?VERSION=>"0.2",
%        ?SCHEMAS=>[
%                    {table_1, 
%                        #{
%                            ?NAME=>table_x,
%                            ?DISC_COPIES=>[node()],
%                            ?DISC_ONLY_COPIES=>[],
%                            ?RAM_COPIES=>[],

%                            ?FIELDS=>[
%                                        {employee_id, 
%                                            #{
%                                                ?NAME=>employee_id,
%                                                ?LABEL=>"Employee ID",
%                                                ?ROLE=>key,
%                                                ?TYPE=>string,
%                                                ?DESCRIPTION=>"table_x key"
%                                            }
%                                        },
%
%                                        {job_id,
%                                            #{
%                                                ?NAME=>job_id,
%                                                ?LABEL=>"Job ID",
%                                                ?ROLE=>field,
%                                                ?TYPE=>integer,
%                                                ?PRIORITY=>mandatory,
%                                                ?DESCRIPTION=>"table_x field 2"
%                                            }
%                                        },
%
%                                        {hourly_wage,
%                                            #{
%                                                ?NAME=>hourly_wage,
%                                                ?LABEL=>"Hourly Wage",
%                                                ?ROLE=>field,
%                                                ?TYPE=>float,
%                                                ?PRIORITY=>mandatory,
%                                                ?DESCRIPTION=>"table_x field 3"
%                                            }
%                                        },
%                        
%                                        {office_id, 
%                                            #{
%                                            ?NAME=>office_id,
%                                            ?LABEL=>"Office Bld ID",
%                                            ?ROLE=>field,
%                                            ?TYPE=>string,
%                                            ?PRIORITY=>optional,
%                                            ?DEFAULT_VALUE=>"not defined",
%                                            ?DESCRIPTION=>"table_x field 4"
%                                            }
%                                        }
%                            ]
%                        }
%                    },
%
%                    {table_2,
%                        
%                        #{
%                            ?NAME=>table_2,
%                            ?DISC_COPIES=>[node()],
%                            ?DISC_ONLY_COPIES=>[],
%                            ?RAM_COPIES=>[],
%
%                            ?FIELDS=>[
%                                        {job_id, 
%                                            #{
%                                                ?NAME=>job_id,
%                                                ?LABEL=>"Job ID",
%                                                ?ROLE=>key,
%                                                ?TYPE=>integer,
%                                                ?PRIORITY=>mandatory,
%                                                ?DESCRIPTION=>"Each job type has its own ID"
%                                            }
%                                        },
%
%                                        {max_job_class,      
%                                            #{
%                                                ?NAME=>max_job_class,
%                                                ?LABEL=>"Max Job Class",
%                                                ?ROLE=>field,
%                                                ?TYPE=>integer,
%                                                ?PRIORITY=>mandatory,
%                                                ?DESCRIPTION=>"Highest job classification qualified to take this job ID"
%                                            }
%                                        },
%
%                                        {max_hourly_wage,
%                  
%                                            #{
%                                                ?NAME=>max_hourly_wage,
%                                                ?LABEL=>"Max Hourly Wage",
%                                                ?ROLE=>field,
%                                                ?TYPE=>float,
%                                                ?PRIORITY=>optional,
%                                                ?DEFAULT_VALUE=>0.0,
%                                                ?DESCRIPTION=>"Highest hourly wage for this job ID"
%                                            }
%                                        },
%           
%                                        {admin_first_name,
%                                            #{
%                                                ?NAME=>admin_first_name,
%                                                ?LABEL=>"Admin First Name",
%                                                ?ROLE=>field,
%                                                ?TYPE=>string,
%                                                ?PRIORITY=>optional,
%                                                ?DEFAULT_VALUE=>"not defined",
%                                                ?DESCRIPTION=>"First name of admin that defined the job ID"
%                                            }
%                                        },
%                  
%                                        {admin_last_name, 
%                                            #{
%                                            
%                                                ?NAME=>admin_last_name,
%                                                ?LABEL=>"Admin Last Name",
%                                                ?ROLE=>field,
%                                                ?TYPE=>string,
%                                                ?PRIORITY=>optional,
%                                                ?DEFAULT_VALUE=>"not defined",
%                                                ?DESCRIPTION=>"Last name of admin that defined the job ID"
%                                            }
%                                        },                                 
%                  
%                                        {status,
%                                            #{
%                                            
%                                                ?NAME=>status,
%                                                ?LABEL=>"Status",
%                                                ?ROLE=>field,
%                                                ?TYPE=>atom,
%                                                ?PRIORITY=>mandatory,
%                                                ?DESCRIPTION=>"Job ID status"
%                                            }
%                                        }
%                            ]
%                        }
%                    }
%        ]
%    }.


