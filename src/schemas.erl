-module(schemas).

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
-define(QUERY_MODULE, query_db).
-define(MODIFY_MODULE, modify_db).
-define(MANAGE_DB_MODULE, manage_db).

% Schema management APIs
-export([new/0, 
         add_schema/2, 
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
         is_field/3, 
         fields/2, 
         field_count/2,
         field_names/2,
         key_name/2, 
         key_type/2,
         field_position/3, 
         get_field_attribute/4,
         set_field_attributes/4, 
         set_field_attribute/5, 
         update_fields/3]).

% Utility APIs
-export([convert_from_string/2, 
         safe_convert_from_string/2, 
         get_type/1,
         build_record/3, 
         build_record_from_specifications/2, 
         validate_record/2, 
         compare_fields_from_specs/2]).





%============================================================
%    SCHEMA MANAGEMENT APIs
%============================================================

%-------------------------------------------------------------
% Function:
% Purpose:  
% Returns:  
%-------------------------------------------------------------
schema_specifications(Module) -> Module:schema_specification().

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
add_schema(SchemaName, SS) ->

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

        true -> {error, schema_exists}
    end.

%-------------------------------------------------------------
% Function:
% Purpose:  
% Returns:  
%-------------------------------------------------------------
get_schema(SchemaName, SS) ->
    
    case schemas(SS) of 
        {error, Reason} -> {error, Reason};
        
        Schemas ->
            case lists:keyfind(SchemaName, 1, Schemas) of 
                {SchemaName, Specifications} -> Specifications;
                false -> {error, {schema_name_not_found, {SchemaName, SS}}}
            end
    end.


%-------------------------------------------------------------
% Function:
% Purpose:  
% Returns:  
%-------------------------------------------------------------
schemas(SS) -> 
    case maps:find(?SCHEMAS, SS) of 
        {ok, Schemas} -> Schemas; 
        error -> {error, {invalid_specifications, SS}}
    end.

%-------------------------------------------------------------
% Function:
% Purpose:  
% Returns:  
%-------------------------------------------------------------
schema_names(SS) -> 
    SchemasList = schemas(SS),
    schema_names([], SchemasList).

schema_names(NameList, []) -> lists:reverse(NameList);
schema_names(NameList, [{Name, _} |T]) ->
    schema_names([Name | NameList], T).


%-------------------------------------------------------------
% Function:
% Purpose:  
% Returns:  boolean()
%-------------------------------------------------------------
is_schema(SchemaName, SS) ->

    case maps:find(?SCHEMAS, SS) of 
        {ok, Schemas} -> lists:keymember(SchemaName, 1, Schemas);
        error -> {error, {invalid_schema_name, SchemaName}} 
    end.

%-------------------------------------------------------------
% Function:
% Purpose:  
% Returns:  
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
% Returns:  boolean()
%-------------------------------------------------------------
set_schema_attributes([], _, SS) -> SS;

set_schema_attributes([{Attribute, Value} | T], SchemaName, SS) ->
    
    case (is_schema_attribute(Attribute, Value) and Attribute /= fields) of 
        true -> ok
    end,

    case Attribute of 
        fields -> update_fields(Value, SchemaName, SS);
        _ ->

            Schema = get_schema(SchemaName, SS),
            UpdatedSchema = maps:update(Attribute, Value, Schema),

            SchemasList = schemas(SS),
            UpdatedSchemasList = lists:keyreplace(SchemaName, 1, SchemasList, UpdatedSchema),
            UpdatedSS = maps:update(?SCHEMAS, UpdatedSchemasList, SS),

            set_schema_attributes(T, SchemaName, UpdatedSS)
    end.

%-------------------------------------------------------------
% Function:
% Purpose: 
% Returns:  
%-------------------------------------------------------------
set_schema_attribute(Attribute, Value, SchemaName, SS) ->

    case is_schema_attribute(Attribute, Value) of 
        true -> ok
    end,

    case Attribute of 
        fields -> update_fields(Value, SchemaName, SS);
        _ ->
            Schema = get_schema(SchemaName, SS),
            UpdatedSchema = maps:update(Attribute, Value, Schema),

            SchemasList = schemas(SS),
            UpdatedSchemasList = lists:keyreplace(SchemaName, 1, SchemasList, UpdatedSchema),
            maps:update(?SCHEMAS, UpdatedSchemasList, SS)
    end.

%-------------------------------------------------------------
% Function:
% Purpose:  
% Returns:  
%------------------------------------------------------------- 
get_schema_attribute(Attribute, SchemaName, SS) -> 
    io:format("~nattribute: ~p~n", [Attribute]),

    case get_schema(SchemaName, SS) of 
        {error, Reason} -> {error, Reason};
        Schema -> maps:get(Attribute, Schema)
    end.


%============================================================
%    FIELD MANAGEMENT APIs
%============================================================

%-------------------------------------------------------------
% Function:
% Purpose:  
% Returns:  
%-------------------------------------------------------------
add_field(FieldName, SchemaName, SS) ->

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

        false -> {error, {schema_name_not_found, {SchemaName, SS}}}
    end.

%-------------------------------------------------------------
% Function:
% Purpose:  
% Returns:  [{FieldName, FieldSpecifications}]
%-------------------------------------------------------------
fields(SchemaName, SS) ->

    Schemas = schemas(SS),
    
    case lists:keyfind(SchemaName, 1, Schemas) of 
        {SchemaName, SchemaSpecifications} ->
  
            case maps:find(?FIELDS, SchemaSpecifications) of 
                {ok, Fields} -> Fields;
                error -> {error, {fields_not_found, {SchemaName, SS}}} 
            end;

        false -> {error, {schema_name_not_found, {SchemaName, SS}}}
    end.


%-------------------------------------------------------------
% Function:
% Purpose:  
% Returns:  
%-------------------------------------------------------------
set_field_attribute(Attribute, Value, FieldName, SchemaName, SS) ->

    % Screen the attribute/value pair.
    case Attribute of
        ?LABEL when is_list(Value) -> ok;
        ?ROLE -> 
        case Value of
            field -> ok;
            key -> ok
        end;

        ?TYPE -> 
            case is_type(Value) of 
                true -> ok
            end;

        ?PRIORITY -> 
        case Value of
            mandatory -> ok;
            optional -> ok
        end;

        ?DEFAULT_VALUE -> ok;
        ?DESCRIPTION when is_list(Value) -> ok
    end,

    Schemas = schemas(SS),
    
    case lists:keyfind(SchemaName, 1, Schemas) of 
        {SchemaName, SchemaSpecifications} -> 
            FieldList = maps:get(?FIELDS, SchemaSpecifications),

            UpdatedFieldList = update_field(Attribute, Value, FieldName, FieldList),

            UpdatedSchemaSpecifications = maps:update(?FIELDS, UpdatedFieldList, SchemaSpecifications),
            UpdatedSchemas = lists:keyreplace(SchemaName, 1, Schemas, {SchemaName, UpdatedSchemaSpecifications}),
            maps:update(?SCHEMAS, UpdatedSchemas, SS);

        false -> {error, {schema_name_not_found, {SchemaName, SS}}}

    end.


%-------------------------------------------------------------
% Function:
% Purpose:  
% Returns:  
%-------------------------------------------------------------
set_field_attributes(AvpList, FieldName, SchemaName, SS) ->

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
                
                    UpdatedFieldSpecifications = set_field_attributes(AvpList, FieldSpecifications),
                    UpdatedFieldList = lists:keyreplace(FieldName, 1, FieldList, {FieldName, UpdatedFieldSpecifications}),

                    UpdatedSchemaSpecifications = maps:update(?FIELDS, UpdatedFieldList, SchemaSpecifications),
                    UpdatedSchemas = lists:keyreplace(SchemaName, 1, Schemas, {SchemaName, UpdatedSchemaSpecifications}),
                    maps:update(?SCHEMAS, UpdatedSchemas, SS);
                
                false -> {error, {field_name_not_found, {FieldName, SS}}}
            end;

        false -> {error, {schema_name_not_found, {SchemaName, SS}}}
    end.


%-------------------------------------------------------------
% Function:
% Purpose:  
% Returns:  
%-------------------------------------------------------------
get_field_attribute(Attribute, FieldName, SchemaName, SS) -> 

    case fields(SchemaName, SS) of 
        {error, Reason} -> {error, Reason};
        
        Fields ->
            case lists:keyfind(FieldName, 1, Fields) of 
                {FieldName, FieldSpecifications} -> maps:get(Attribute, FieldSpecifications);
                false -> {error, {field_name_not_found, {FieldName, SS}}}
            end
    end.


%-------------------------------------------------------------
% Function:
% Purpose:  
% Returns:  boolean()
%-------------------------------------------------------------
is_field(FieldName, SchemaName, SS) ->

    Fields = fields(SchemaName, SS),
    lists:keymember(FieldName, 1, Fields).


%-------------------------------------------------------------
% Function:
% Purpose:  
% Returns:  
%------------------------------------------------------------- 
update_fields(FieldList, SchemaName, SS) -> 

    %% First, validate the field list
    case is_field_list(FieldList) of 
        true -> 
            SchemasList = schemas(SS),

            case lists:keyfind(SchemaName, 1, SchemasList) of

                {SchemaName, SchemaSpecifications} ->
                    UpdatedSchemaSpecifications = maps:update(?FIELDS, FieldList, SchemaSpecifications),

                    UpdatedSchemasList = lists:keyreplace(SchemaName, 1, SchemasList, {SchemaName, UpdatedSchemaSpecifications}),
                    maps:update(?SCHEMAS, UpdatedSchemasList, SS);

                false -> {error, {schema_name_not_found, {SchemaName, SS}}}
            end;

        false -> {error, {bad_field_list, FieldList}}
    end.

%-------------------------------------------------------------
% Function:
% Purpose:  
% Returns:  
%-------------------------------------------------------------
field_count(SchemaName, SS) ->

    case fields(SchemaName, SS) of 
        {error, _} -> 0;
        Fields -> length(Fields) 
    end. 

%-------------------------------------------------------------
% Function:
% Purpose:  
% Returns:  
%-------------------------------------------------------------
field_names(SchemaName, SS) -> 
    case fields(SchemaName, SS) of 
        {error, Reason} -> {error, Reason};
        Fields -> field_names_next(Fields, [])
    end. 
%-------------------------------------------------------------
% Function:
% Purpose:  
% Returns:  
%-------------------------------------------------------------
key_name(SchemaName, SS) -> 

    case fields(SchemaName, SS) of 
        {error, Reason} -> {error, Reason};
         [Key|_] -> Key
    end.

%-------------------------------------------------------------
% Function:
% Purpose:  
% Returns:  
%-------------------------------------------------------------
key_type(SchemaName, SS) -> 

    case field_names(SchemaName, SS) of 
        {error, Reason} -> {error, Reason};
         [Key|_] -> get_field_attribute(type, Key, SchemaName, SS)
    end.

%-------------------------------------------------------------
% Function:
% Purpose:  
% Returns:  
%-------------------------------------------------------------
field_position(FieldName, SchemaName, SS) -> get_field_attribute(?POSITION, FieldName, SchemaName, SS).



%============================================================
%    CODE GENERATION APIs
%============================================================

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns:  
%-------------------------------------------------------------
generate(Module, SS) -> generate(Module, ".", ".", SS).

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns:  
%-------------------------------------------------------------
generate(Module, SrcPath, HrlPath, SS) ->

    case utilities:is_unquoted_atom(Module) of 
        true ->
            case file:open(HrlPath ++ "/" ++ atom_to_list(Module) ++ ".hrl", [write]) of 
                {ok, HrlIoDevice} ->

                    generate_records(SS, HrlIoDevice),

                    case file:open(SrcPath ++ "/" ++ atom_to_list(Module) ++ ".erl", [write]) of
                        {ok, SrcIoDevice}  -> 
                            
                            io:format(SrcIoDevice, "-module(~p).~n", [Module]),

                            

                            io:format(SrcIoDevice, "~n", []),
                            io:format(SrcIoDevice, "-export([schema_specifications/0]).~n", []),

                            io:format(SrcIoDevice, "-export([install/0, install/1, start/0, stop/0, table_size/1, table_sizes/0]).~n", []),
                            io:format(SrcIoDevice, "-export([schema_names/0, is_schema/1, is_field/2, schemas/0, get_schema/1, get_schema_attribute/2]).~n", []),
                            io:format(SrcIoDevice, "-export([fields/1, field_count/1, field_names/1, key_name/1, key_type/1, field_position/2, get_field_attribute/3]).~n", []),
                            io:format(SrcIoDevice, "-export([read/2, select/4, select_or/6, select_and/6, build_matchhead/1]).~n", []),
                            io:format(SrcIoDevice, "-export([add/3, delete/2, clear_all_tables/0]).~n", []),
                            io:format(SrcIoDevice, "-export([build_record_from_specifications/1, validate_record/1]).~n", []),
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
                            generate_function(table_size, "SchemaName", ?MANAGE_DB_MODULE, SrcIoDevice),
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
                            generate_spec_function(field_names, "SchemaName", ?MODULE, SrcIoDevice),
                            generate_spec_function(key_name, "SchemaName", ?MODULE, SrcIoDevice),
                            generate_spec_function(key_type, "SchemaName", ?MODULE, SrcIoDevice),
                            generate_spec_function(field_position, "FieldNAme", "SchemaName", ?MODULE, SrcIoDevice),
                            generate_spec_function(get_field_attribute, "Attribute", "FieldName", "SchemaName", ?MODULE, SrcIoDevice),

                            io:format(SrcIoDevice, "~n~n", []),
                            io:format(SrcIoDevice, "%-------------------------------------------------------~n",[]),
                            io:format(SrcIoDevice, "%                     Query Functions~n",[]),
                            io:format(SrcIoDevice, "%-------------------------------------------------------~n",[]),

                            generate_function(read, "Table", "Key", ?QUERY_MODULE, SrcIoDevice),

                            generate_spec_function(select, "Table", "Field", "Oper", "Value", ?QUERY_MODULE, SrcIoDevice),
                            generate_spec_function(select_or, "Table", "Field", "Oper1", "Value1", "Oper2", "Value2", ?QUERY_MODULE, SrcIoDevice),
                            generate_spec_function(select_and, "Table", "Field", "Oper1", "Value1", "Oper2", "Value2", ?QUERY_MODULE, SrcIoDevice),
                            generate_spec_function(build_matchhead, "Table", ?QUERY_MODULE, SrcIoDevice),

                            io:format(SrcIoDevice, "~n~n", []),
                            io:format(SrcIoDevice, "%-------------------------------------------------------~n",[]),
                            io:format(SrcIoDevice, "%                     Modify Functions~n",[]),
                            io:format(SrcIoDevice, "%-------------------------------------------------------~n",[]),

                            generate_function(add, "Table", "Key", "Data", ?MODIFY_MODULE, SrcIoDevice),
                            generate_function(delete, "Table", "Key", ?MODIFY_MODULE, SrcIoDevice),
                            generate_function(clear_all_tables, ?MODIFY_MODULE, SrcIoDevice),

                            io:format(SrcIoDevice, "~n~n", []),
                            io:format(SrcIoDevice, "%-------------------------------------------------------~n",[]),
                            io:format(SrcIoDevice, "%                     Utility Functions~n",[]),
                            io:format(SrcIoDevice, "%-------------------------------------------------------~n",[]),

                            generate_spec_function(build_record_from_specifications, "SchemaName", ?MODULE, SrcIoDevice),
                            generate_spec_function(validate_record, "Record", ?MODULE, SrcIoDevice),
                           
                            file:close(SrcIoDevice);

                        {error, Reason} -> {error, Reason}
                    end;

                {error, Reason} -> {error, Reason}
            end;

        false -> {error, {invalid_module_name, Module}}
    end.


%============================================================
%    UTILITY APIs
%============================================================

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: converts the value or crashes
%-------------------------------------------------------------
safe_convert_from_string(Value, Type) ->

  case is_list(Value) of
    true ->
      case Type of
        integer -> utilities:string_to_integer(Value);
        float -> utilities:string_to_float(Value);
        string -> {ok, Value};
        list -> {ok, Value};
        atom -> {ok, list_to_atom(Value)};
        tuple -> utilities:string_to_tuple(Value);
        term -> {ok, Value};
        _ -> {error, {unknown_type, Type}}
      end;

    false -> {error, {bad_input, Value}}
  end.


%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: converts the value or crashes
%-------------------------------------------------------------
convert_from_string(Value, Type) when is_list(Value) ->

  case Type of

    integer -> 

      case utilities:string_to_integer(Value) of
        {ok, Converted} -> Converted
      end;

    float -> 
      
      case utilities:string_to_float(Value) of
        {ok, Converted} -> Converted
      end;

    string -> Value;
    list -> Value;

    atom -> list_to_atom(Value);
    
    tuple -> 
      case utilities:string_to_tuple(Value) of
        {ok, Converted} -> Converted
      end;

    term -> Value
  end.

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns:  
%-------------------------------------------------------------
build_record(SchemaName, Key, Data) -> list_to_tuple([SchemaName | [Key | tuple_to_list(Data)]]).

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns:  
%-------------------------------------------------------------
build_record_from_specifications(SchemaName, SS) -> tuple_to_list([SchemaName, field_names(SchemaName, SS)]).


%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns:  
%-------------------------------------------------------------
validate_record(Record, SS) -> 

    % 1. Convert record tuple to list and take out the head, that is the schema name.
    % 2. Extract the corresponding schema field specifications.
    % 3. Cycle through each field and validate name and position.

    RecordList = tuple_to_list(Record),
    [SchemaName | RecordFields] = RecordList,

    case is_schema(SchemaName, SS) of 
        true ->
            SchemaFields = field_names(SchemaName, SS),
            compare_field_name_order(RecordFields, SchemaFields);
        false -> false
    end.

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns:  
%-------------------------------------------------------------
compare_field_name_order([], []) -> true;
compare_field_name_order(_, []) -> false;
compare_field_name_order([], _) -> false;
compare_field_name_order([Next1 | T1], [Next2 | T2]) ->

    case (Next1 == Next2) of 
        true -> compare_field_name_order(T1, T2);

        false -> false 
    end.

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
create_schema(SchemaName)-> 

  S1 = maps:put(?NAME, SchemaName, maps:new()),
  S2 = maps:put(type, set, S1),
  S3 = maps:put(disc_copies, [], S2),
  S4 = maps:put(disc_only_copies, [], S3),
  S5 = maps:put(ram_copies, [], S4),
  
  maps:put(?FIELDS, [], S5).

%-------------------------------------------------------------
% Function:
% Purpose:  
% Returns:  
%-------------------------------------------------------------
create_field(Name) ->

  F1 = maps:put(?NAME, Name, maps:new()),
  F2 = maps:put(?LABEL, "", F1),
  F3 = maps:put(?ROLE, field, F2),
  F4 = maps:put(?TYPE, not_defined, F3),
  F5 = maps:put(?POSITION, 0, F4),
  F6 = maps:put(?PRIORITY, mandatory, F5),
  F7 = maps:put(?DEFAULT_VALUE, not_defined, F6),
  maps:put(?DESCRIPTION, "", F7).

%-------------------------------------------------------------
% Function:
% Purpose:  
% Returns:  
%-------------------------------------------------------------
add_field(FieldName, FieldList) ->

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
    end.

%-------------------------------------------------------------
% Function:
% Purpose:  Sets field attributes except for position and role,
%           they are unchangeable.
% Returns:  
%-------------------------------------------------------------
update_field(Attribute, Value, FieldName, FieldList) ->

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
    end.

      
%-------------------------------------------------------------
% Function:
% Purpose:  
% Returns:  
%-------------------------------------------------------------
field_names_next([], FinalList) -> lists:reverse(FinalList);
field_names_next([{FieldName, _} | T], CompiledList) -> field_names_next(T, [FieldName | CompiledList]).

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

set_field_attributes([{?LABEL, NewLabel} | T], FieldSpecifications) when is_list(NewLabel) -> 
    set_field_attributes(T, maps:update(?LABEL, NewLabel, FieldSpecifications));

set_field_attributes([{?ROLE, NewRole} | T], FieldSpecifications) -> 
    case NewRole of
        key -> ok;
        field -> ok 
    end,
    set_field_attributes(T, maps:update(?LABEL, NewRole, FieldSpecifications));

set_field_attributes([{?TYPE, Type} | T], FieldSpecifications) -> 
    case is_type(Type) of 
        true -> set_field_attributes(T, maps:update(?TYPE, Type, FieldSpecifications));
        false -> {error, {invalid_argument, Type}} 
    end;

set_field_attributes([{?PRIORITY, Priority} | T], FieldSpecifications) -> 
    case Priority of
        mandatory -> ok;
        optional -> ok 
    end,
    set_field_attributes(T, maps:update(?PRIORITY, Priority, FieldSpecifications));

set_field_attributes([{?DEFAULT_VALUE, Value} | T], FieldSpecifications) -> 
    set_field_attributes(T, maps:update(?DEFAULT_VALUE, Value, FieldSpecifications));

set_field_attributes([{?DESCRIPTION, Description} | T], FieldSpecifications) when is_list(Description) -> 
    set_field_attributes(T, maps:update(?DESCRIPTION, Description, FieldSpecifications)).


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
generate_function(FunctionName, Arg1, BaseModule, IoDevice) ->
    io:format(IoDevice, "~n~p(~s) -> ~p:~p(~s).~n", 
        [FunctionName, Arg1, BaseModule, FunctionName, Arg1]).

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
generate_function(FunctionName, Arg1, Arg2, Arg3, BaseModule, IoDevice) ->
    io:format(IoDevice, "~n~p(~s, ~s, ~s) -> ~p:~p(~s, ~s, ~s).~n", 
        [FunctionName, Arg1, Arg2, Arg3, BaseModule, FunctionName, Arg1, Arg2, Arg3]).

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


