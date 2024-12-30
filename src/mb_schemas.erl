-module(mb_schemas).
-include("../include/mb.hrl").


-define(QUERY_MODULE, mb_db_query).
-define(MODIFY_MODULE, mb_db_edit).
-define(MANAGE_DB_MODULE, mb_db_management).

% Schema management APIs
-export([new/0, 
         new/4,
         is_ssg_attribute/1,
         validate_ssg_attribute/2,
         set_ssg_name/2,
         set_ssg_owner/2,
         set_ssg_email/2,
         set_ssg_description/2,
         get_ssg_attributes/1,
         add_schema/2, 
         delete_schema/2,
         is_schema_attribute/1,
         validate_schema_attribute/2,

         set_schema_name/3,
         set_schema_type/3,
         set_schema_ram_copies/3,
         set_schema_disc_copies/3,
         set_schema_disc_only_copies/3,
         set_schema_description/3,

         get_schema_type/2,
         get_schema_ram_copies/2,
         get_schema_disc_copies/2,
         get_schema_disc_only_copies/2,
         get_schema_description/2,

         set_schema_attributes/3,
         set_schema_attribute/4,
         get_schema_attribute/3,

         is_schema/2, 
         schemas/1, 
         get_schema/2, 
         get_ssg/1, 
         schema_names/1]).


-export([generate/2, generate/4]).

% Field Management APIs
-export([add_field/3, 
         is_field_attribute/1,
         validate_field_attribute/5,

         set_field_name/4,
         set_field_description/4,
         set_field_type/4,
         set_field_priority/4,
         set_field_default_value/4,
         set_field_label/4,

         get_field_description/3,
         get_field_type/3,
         get_field_priority/3,
         get_field_default_value/3,
         get_field_label/3,
         get_field_position/3,

         set_field_attributes/4,
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
         move_field/4,
         make_key/3,
         set_field_attribute/5]).

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
% Executes the callback and retrieves its schema spec group.
%-------------------------------------------------------------
-spec get_ssg(mb_module()) -> mb_ssg() | mb_error().
%-------------------------------------------------------------
get_ssg(Module) when (Module /= ?MODULE), is_atom(Module) -> Module:get_ssg();
get_ssg(Module) -> {error, {invalid_argument, Module}}.


%-------------------------------------------------------------
% Allocates a new schema specifications group with default
% schema attrbitues.
%-------------------------------------------------------------
-spec new() -> mb_ssg().
%-------------------------------------------------------------
new() -> new([], [], [], []).


%-------------------------------------------------------------
% Allocates a new schema specifications group.
%-------------------------------------------------------------
-spec new(mb_ssg_name(), string(), string(), string()) -> mb_ssg().
%-------------------------------------------------------------
new(Name, Owner, Email, Description) ->
    #{?VERSION=>?CURRENT_VERSION,
      ?NAME=>Name,
      ?CREATED=>calendar:local_time(),
      ?OWNER=>Owner,
      ?EMAIL=>Email,
      ?DESCRIPTION=>Description,
      ?SCHEMAS=>[]}.
 
%-------------------------------------------------------------
%  
%-------------------------------------------------------------
-spec is_ssg_attribute(mb_ssg_attribute()) -> boolean().
%-------------------------------------------------------------
is_ssg_attribute(Attribute) when is_atom(Attribute) ->

    case Attribute of 
        ?VERSION -> true;
        ?NAME -> true;
        ?CREATED -> true;
        ?OWNER -> true;
        ?EMAIL -> true;
        ?DESCRIPTION -> true;
        ?SCHEMAS -> true;
        _ -> false
    end;

is_ssg_attribute(_) -> false.

%-------------------------------------------------------------
%  
%-------------------------------------------------------------
-spec validate_ssg_attribute(mb_ssg_attribute(), term()) -> boolean().
%-------------------------------------------------------------
validate_ssg_attribute(Attribute, Value) -> 

    case Attribute of 
        ?VERSION -> mb_utilities:is_printable_string(Value);
        ?NAME -> is_atom(Value);
        ?CREATED -> 
            case Value of 
                {{YY, MM, DD}, {HH, MM, SS}} -> is_integer(YY) andalso 
                                                is_integer(MM) andalso
                                                is_integer(DD) andalso
                                                is_integer(HH) andalso
                                                is_integer(MM) andalso
                                                is_integer(SS);
                _ -> false 
            end;

        ?OWNER -> mb_utilities:is_printable_string(Value);
        ?EMAIL -> mb_utilities:is_email(Value);
        ?DESCRIPTION -> mb_utilities:is_printable_string(Value);
        ?SCHEMAS -> is_list(Value); %% TODO: need proper validation
        _ -> false
    end.

%-------------------------------------------------------------
%  
%-------------------------------------------------------------
-spec set_ssg_name(mb_ssg_name(), mb_ssg()) -> mb_ssg() | mb_error().
%-------------------------------------------------------------
set_ssg_name(Name, SSG) when is_atom(Name), is_map(SSG) -> set_ssg_attribute(?NAME, Name, SSG).

%-------------------------------------------------------------
%  
%-------------------------------------------------------------
-spec set_ssg_owner(string(), mb_ssg()) -> mb_ssg() | mb_error().
%-------------------------------------------------------------
set_ssg_owner(Owner, SSG) -> set_ssg_attribute(?OWNER, Owner, SSG).

%-------------------------------------------------------------
%  
%-------------------------------------------------------------
-spec set_ssg_email(string(), mb_ssg()) -> mb_ssg() | mb_error().
%-------------------------------------------------------------
set_ssg_email(Email, SSG) -> set_ssg_attribute(?EMAIL, Email, SSG).

%-------------------------------------------------------------
%  
%-------------------------------------------------------------
-spec set_ssg_description(string(), mb_ssg()) -> mb_ssg() | mb_error().
%-------------------------------------------------------------
set_ssg_description(Description, SSG) -> set_ssg_attribute(?DESCRIPTION, Description, SSG).

%-------------------------------------------------------------
%  Restricted to a subset of the SSG attributes: name, owner, email, and description.
%-------------------------------------------------------------
-spec set_ssg_attribute(mb_ssg_attribute(), term(), mb_ssg()) -> mb_ssg() | mb_error().
%-------------------------------------------------------------
set_ssg_attribute(Attribute, Value, SSG) when is_map(SSG) -> 

    case validate_ssg_attribute(Attribute, Value) of 
        true ->
            case ((Attribute == ?NAME) or (Attribute == ?OWNER) or (Attribute == ?EMAIL) or (Attribute == ?DESCRIPTION)) of 
                true ->
                    case maps:find(Attribute, SSG) of 
                        {ok, _} -> maps:update(Attribute, Value, SSG); 
                        error -> {error, {invalid_specifications, SSG}}
                    end;

                false -> {error, {invalid_request, Attribute}}
            end;
        false -> {error, {invalid_ssg_attribute_value_pair, {Attribute, Value}}}
    end;

set_ssg_attribute(Attribute, _Value, SSG) -> {error, {invalid_argument, {Attribute, SSG}}}.

%-------------------------------------------------------------
%  Returns the SSG attributes except the schemas.
%-------------------------------------------------------------
-spec get_ssg_attributes(mb_ssg()) -> mb_ssg() | mb_error().
%-------------------------------------------------------------
get_ssg_attributes(SSG) when is_map(SSG) ->

    case maps:find(?VERSION, SSG) of 
        {ok, Version} -> 
            case maps:find(?NAME, SSG) of
                {ok, Name} ->
                    case maps:find(?CREATED, SSG) of 
                        {ok, Created} ->
                            case maps:find(?OWNER, SSG) of 
                                {ok, Owner} ->
                                    case maps:find(?EMAIL, SSG) of 
                                        {ok, Email} ->
                                            case maps:find(?DESCRIPTION, SSG) of
                                                {ok, Description} -> [{?VERSION, Version}, {?NAME, Name}, {?CREATED, Created}, {?OWNER, Owner}, {?EMAIL, Email}, {?DESCRIPTION, Description}];
                                                _ -> {error, {invalid_specifications, SSG}} 
                                            end;
                                        _ -> {error, {invalid_specifications, SSG}} 
                                    end;
                                _ -> {error, {invalid_specifications, SSG}}
                            end;
                        _ -> {error, {invalid_specifications, SSG}} 
                    end;
                _ -> {error, {invalid_specifications, SSG}} 
            end;
        _ -> {error, {invalid_specifications, SSG}} 
    end;

get_ssg_attributes(SSG) -> {error, {invalid_specifications, SSG}}.



%-------------------------------------------------------------
% Add a new schema to the specifications.
%-------------------------------------------------------------
-spec add_schema(mb_schema_name(), mb_ssg()) -> mb_ssg() | mb_error().
%-------------------------------------------------------------
add_schema(SchemaName, SSG) when is_map(SSG) ->

    % Get the list of schemas and their specification maps.
    SchemasList = schemas(SSG),

    % We will only proceed with the add if the
    % get_schema name does not exist.
    case lists:keymember(SchemaName, 1, SchemasList) of
        false ->
            % Create a default get_schema specification map for this get_schema.
            Schema = create_schema(SchemaName),

            % Update the schemas list, the map, and return the updated map.
            UpdatedSchemasList = SchemasList ++ [{SchemaName, Schema}],
            maps:update(?SCHEMAS, UpdatedSchemasList, SSG);

        true -> {error, {schema_exists, SchemaName}}
    end;

add_schema(_, SSG) -> {error, {invalid_argument, SSG}}.

%-------------------------------------------------------------
% Delete a schema from the specifications. 
%-------------------------------------------------------------
-spec delete_schema(mb_schema_name(), mb_ssg()) -> mb_ssg() | mb_error().
%-------------------------------------------------------------
delete_schema(SchemaName, SSG) when is_map(SSG) ->

    SchemasList = schemas(SSG),
    UpdatedSchemasList = lists:keydelete(SchemaName, 1, SchemasList),
    maps:update(?SCHEMAS, UpdatedSchemasList, SSG);

delete_schema(_, SSG) -> {error, {invalid_argument, SSG}}.

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec get_schema(mb_schema_name(), mb_ssg()) -> mb_schema_spec() | mb_error().
%-------------------------------------------------------------
get_schema(SchemaName, SSG) when is_map(SSG) ->

    case schemas(SSG) of 
        {error, Reason} -> {error, Reason};
        
        Schemas ->
            case lists:keyfind(SchemaName, 1, Schemas) of 
                {SchemaName, Specifications} -> Specifications;
                false -> {error, {schema_name_not_found, SchemaName}}
            end
    end;

get_schema(_, SSG) -> {error, {invalid_argument, SSG}}.



%-------------------------------------------------------------
%   
%-------------------------------------------------------------
-spec schemas(mb_ssg()) -> list() | mb_error().
%-------------------------------------------------------------
schemas(SSG) when is_map(SSG) ->

    case maps:find(?SCHEMAS, SSG) of 
        {ok, Schemas} -> Schemas; 
        error -> {error, {invalid_ssg_format, SSG}}
    end;

schemas(SSG) -> {error, {invalid_argument, SSG}}.

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec schema_names(mb_ssg()) -> list() | mb_error().
%-------------------------------------------------------------
schema_names(SSG) when is_map(SSG) -> 
    case schemas(SSG) of 
        {error, Reason} -> {error, Reason};
        SchemasList -> schema_names([], SchemasList)
    end;

schema_names(SSG) -> {error, {invalid_argument, SSG}}.


%-------------------------------------------------------------
-spec schema_names(list(), list()) -> list() | mb_error().
%-------------------------------------------------------------
schema_names(NameList, []) -> lists:reverse(NameList);
schema_names(NameList, [{Name, _} |T]) ->
    schema_names([Name | NameList], T);

schema_names(_, List) -> {error, {invalid_ssg_format, List}}.


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec is_schema(mb_schema_name(), mb_ssg()) -> boolean() | mb_error().
%-------------------------------------------------------------
is_schema(SchemaName, SSG) when is_map(SSG) ->

    case maps:find(?SCHEMAS, SSG) of 
        {ok, Schemas} -> lists:keymember(SchemaName, 1, Schemas);
        error -> {error, {invalid_ssg_format, SSG}} 
    end;

is_schema(_, SSG) -> {error, {invalid_argument, SSG}}.


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec is_schema_attribute(mb_schema_attribute()) -> boolean() | mb_error().
%-------------------------------------------------------------
is_schema_attribute(Attribute) when is_atom(Attribute) -> 

   case Attribute of 
        ?NAME -> true;
        ?DESCRIPTION -> true;
        ?SCHEMA_TYPE -> true;
        ?RAM_COPIES -> true;
        ?DISC_COPIES -> true;
        ?DISC_ONLY_COPIES -> true;
        ?FIELDS -> true;
        _ -> false
    end;

is_schema_attribute(Attribute) -> {error, {invalid_argument, Attribute}}.

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec validate_schema_attribute(mb_schema_attribute(), term()) -> boolean() | mb_error().
%-------------------------------------------------------------
validate_schema_attribute(Attribute, Value) when is_atom(Attribute) -> 

   case Attribute of 
        ?NAME -> is_atom(Value);
        ?DESCRIPTION -> mb_utilities:is_printable_string(Value);
        ?SCHEMA_TYPE -> 
            case Value of 
                set -> true;
                ordered_set -> true;
                bag -> true;
                _ -> false 
            end;

        ?DISC_COPIES -> mb_utilities:is_node_name_list(Value);
        ?DISC_ONLY_COPIES -> mb_utilities:is_node_name_list(Value);
        ?RAM_COPIES -> mb_utilities:is_node_name_list(Value);
        ?FIELDS -> is_field_list(Value); %% TODO: field list validation is weak
        _ -> false
    end;

validate_schema_attribute(Attribute, _) -> {error, {invalid_argument, Attribute}}.


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec set_schema_name(mb_schema_name(), mb_schema_name(), mb_ssg()) -> mb_ssg() | mb_error().
%-------------------------------------------------------------
set_schema_name(NewName, OldName, SSG) -> set_schema_attribute(?NAME, NewName, OldName, SSG).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec set_schema_type(mb_schema_type(), mb_schema_name(), mb_ssg()) -> mb_ssg() | mb_error().
%-------------------------------------------------------------
set_schema_type(Type, SchemaName, SSG) -> set_schema_attribute(?SCHEMA_TYPE, Type, SchemaName, SSG).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec set_schema_ram_copies(list(), mb_schema_name(), mb_ssg()) -> mb_ssg() | mb_error().
%-------------------------------------------------------------
set_schema_ram_copies(NodesList, SchemaName, SSG) -> set_schema_attribute(?RAM_COPIES, NodesList, SchemaName, SSG).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec set_schema_disc_copies(list(), mb_schema_name(), mb_ssg()) -> mb_ssg() | mb_error().
%-------------------------------------------------------------
set_schema_disc_copies(NodesList, SchemaName, SSG) -> set_schema_attribute(?DISC_COPIES, NodesList, SchemaName, SSG).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec set_schema_disc_only_copies(list(), mb_schema_name(), mb_ssg()) -> mb_ssg() | mb_error().
%-------------------------------------------------------------
set_schema_disc_only_copies(NodesList, SchemaName, SSG) -> set_schema_attribute(?DISC_ONLY_COPIES, NodesList, SchemaName, SSG).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec set_schema_description(string(), mb_schema_name(), mb_ssg()) -> mb_ssg() | mb_error().
%-------------------------------------------------------------
set_schema_description(Description, SchemaName, SSG) -> set_schema_attribute(?DESCRIPTION, Description, SchemaName, SSG).





%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec get_schema_type(mb_schema_name(), mb_ssg()) -> mb_ssg() | mb_error().
%-------------------------------------------------------------
get_schema_type(SchemaName, SSG) -> get_schema_attribute(?SCHEMA_TYPE, SchemaName, SSG).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec get_schema_ram_copies(mb_schema_name(), mb_ssg()) -> mb_ssg() | mb_error().
%-------------------------------------------------------------
get_schema_ram_copies(SchemaName, SSG) -> get_schema_attribute(?RAM_COPIES, SchemaName, SSG).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec get_schema_disc_copies( mb_schema_name(), mb_ssg()) -> mb_ssg() | mb_error().
%-------------------------------------------------------------
get_schema_disc_copies(SchemaName, SSG) -> get_schema_attribute(?DISC_COPIES, SchemaName, SSG).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec get_schema_disc_only_copies(mb_schema_name(), mb_ssg()) -> mb_ssg() | mb_error().
%-------------------------------------------------------------
get_schema_disc_only_copies(SchemaName, SSG) -> get_schema_attribute(?DISC_ONLY_COPIES, SchemaName, SSG).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec get_schema_description(mb_schema_name(), mb_ssg()) -> mb_ssg() | mb_error().
%-------------------------------------------------------------
get_schema_description(SchemaName, SSG) -> get_schema_attribute(?DESCRIPTION, SchemaName, SSG).




%-------------------------------------------------------------
% Sets the schema attributes in batch but not fields. 
%-------------------------------------------------------------
-spec set_schema_attributes(mb_schema_avp_list(), mb_schema_name(), mb_ssg()) -> mb_ssg() | mb_error().
%-------------------------------------------------------------
set_schema_attributes([], _, SSG) when is_map(SSG) -> SSG;

set_schema_attributes([{Attribute, Value} | T], SchemaName, SSG) when is_map(SSG) ->

    case set_schema_attribute(Attribute, Value, SchemaName, SSG) of 
        {error, Reason} -> {error, Reason};
        UpdatedSSG -> set_schema_attributes(T, SchemaName, UpdatedSSG) 
    end;

set_schema_attributes(AvpList, SchemaName, SSG) -> {error, {invalid_argument, {AvpList, SchemaName, SSG}}}.

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
set_schema_attribute(Attribute, Value, SchemaName, SSG) when is_map(SSG) -> 

    case validate_schema_attribute(Attribute, Value) of
        true ->
            case Attribute /= ?FIELDS of 
                true ->
                    case Attribute == ?NAME of 
                        true -> rename_schema(Value, SchemaName, SSG);
                        false ->
                            case get_schema(SchemaName, SSG) of
                                {error, Reason} -> {error, Reason};
                                Schema -> update_schema_attribute(Attribute, Value, SchemaName, Schema, SSG)
                            end
                    end;

                false -> {error, {not_permitted, Attribute}}
            end;

        false -> {error, {invalid_schema_attribute_value_pair, {Attribute, Value}}}
    end;

set_schema_attribute(_ , _, _, SSG) -> {error, {invalid_argument, SSG}}.


%-------------------------------------------------------------
% 
%------------------------------------------------------------- 
-spec get_schema_attribute(mb_schema_attribute(), mb_schema_name(), mb_ssg()) -> term() | mb_error().
%------------------------------------------------------------- 
get_schema_attribute(Attribute, SchemaName, SSG) when is_map(SSG) -> 

    case is_schema_attribute(Attribute) of
        true ->
            case get_schema(SchemaName, SSG) of 
                {error, Reason} -> {error, Reason};
                Schema -> 
                    case maps:find(Attribute, Schema) of 
                        {ok, Value} -> Value;
                        error -> {error, {invalid_argument, SSG}}
                    end
            end;
        false -> {error, {invalid_attribute, Attribute}}
    end;

get_schema_attribute(_, _, SSG) -> {error, {invalid_argument, SSG}}.



%============================================================
%    FIELD MANAGEMENT APIs
%============================================================

%-------------------------------------------------------------
%  
%-------------------------------------------------------------
-spec add_field(mb_field_name(), mb_schema_name(), mb_ssg()) -> mb_ssg() | mb_error().
%-------------------------------------------------------------
add_field(FieldName, SchemaName, SSG) when is_map(SSG) ->

    Schemas = schemas(SSG),

    case lists:keyfind(SchemaName, 1, Schemas) of 
        {SchemaName, SchemaSpecifications} ->
                
            case maps:find(?FIELDS, SchemaSpecifications) of 
                {ok, FieldList} ->
                    case add_field(FieldName, FieldList) of 
                        {error, Reason} -> {error, Reason};

                        UpdatedFieldList ->
                            UpdatedSchemaSpecifications = maps:update(?FIELDS, UpdatedFieldList, SchemaSpecifications),
                            UpdatedSchemas = lists:keyreplace(SchemaName, 1, Schemas, {SchemaName, UpdatedSchemaSpecifications}),
                            maps:update(?SCHEMAS, UpdatedSchemas, SSG)
                    end; 

                error -> {error, {invalid_argument, SSG}}
            end;

        false -> {error, {schema_name_not_found, SchemaName}}
    end;

add_field(FieldName, SchemaName, SSG) -> {error, {invalid_argument, {FieldName, SchemaName, SSG}}}.


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec get_field(mb_field_name(), mb_schema_name(), mb_ssg()) -> mb_field_spec() | mb_error().
%-------------------------------------------------------------
get_field(FieldName, SchemaName, SSG) when is_map(SSG) ->
    
    case fields(SchemaName, SSG) of 
        {error, Reason} -> {error, Reason};
        FieldsList ->
            case lists:keyfind(FieldName, 1, FieldsList) of
                false -> {error, {not_found, FieldName}};
                FieldSpec -> FieldSpec 
            end
    end;

get_field(_, _, SSG) -> {error, {invalid_argument, SSG}}.

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec fields(mb_schema_name(), mb_ssg()) -> mb_field_spec_list() | mb_error().
%-------------------------------------------------------------
fields(SchemaName, SSG) when is_map(SSG) ->

    Schemas = schemas(SSG),
    
    case lists:keyfind(SchemaName, 1, Schemas) of 
        {SchemaName, SchemaSpecifications} ->
            case maps:find(?FIELDS, SchemaSpecifications) of
                {ok, Fields} -> Fields;
                _ -> {error, {invalid_specifications, SSG}}
            end;
        false -> {error, {schema_name_not_found, SchemaName}}
    end;

fields(_, SSG) -> {error, {invalid_argument, SSG}}.


%-------------------------------------------------------------
%
%-------------------------------------------------------------
-spec is_field_attribute(mb_field_attribute()) -> boolean().
%-------------------------------------------------------------
is_field_attribute(Attribute) ->

    case Attribute of 
        ?NAME -> true;
        ?LABEL -> true;
        ?DESCRIPTION -> true;
        ?ROLE -> true;
        ?FIELD_TYPE -> true;
        ?POSITION -> true;
        ?PRIORITY -> true;
        ?DEFAULT_VALUE -> true;
        _ -> false 
    end.

%-------------------------------------------------------------
%
%-------------------------------------------------------------
-spec validate_field_attribute(mb_field_attribute(), term(), mb_field_name(), mb_schema_name(), mb_ssg()) -> boolean().
%-------------------------------------------------------------
validate_field_attribute(Attribute, Value, FieldName, SchemaName, SSG) ->

    case Attribute of 
        
        ?NAME -> is_atom(Value);
        ?LABEL -> mb_utilities:is_printable_string(Value);
        ?DESCRIPTION -> mb_utilities:is_printable_string(Value);

        ?ROLE -> 
            case Value of 
                key -> true;
                field -> true;
                _ -> false 
            end;
               
        ?FIELD_TYPE -> is_type(Value);
        ?POSITION   -> is_integer(Value) andalso Value > 0; 
        ?PRIORITY -> 
            case Value of 
                mandatory -> true;
                optional -> true;
                _ -> false 
            end;

        ?DEFAULT_VALUE -> 

            case get_field_attribute(?FIELD_TYPE, FieldName, SchemaName, SSG) of 
                {error, _} -> false;
                term -> true;
                Type ->
                    case get_type(Value) of 
                        Type -> true; 
                        _ -> false %% default value does not match field type
                    end
            end;

        _ -> false

    end.

%-------------------------------------------------------------
% Sets the schema attributes in batch but not fields. 
%-------------------------------------------------------------
-spec set_field_attributes(mb_field_avp_list(), mb_field_name(), mb_schema_name(), mb_ssg()) -> mb_ssg() | mb_error().
%-------------------------------------------------------------
set_field_attributes([], _, _, SSG) when is_map(SSG) -> SSG;

set_field_attributes([{Attribute, Value} | T], FieldName, SchemaName, SSG) when is_map(SSG) ->

    case set_field_attribute(Attribute, Value, FieldName, SchemaName, SSG) of 
        {error, Reason} -> {error, Reason};
        UpdatedSSG -> set_field_attributes(T, FieldName, SchemaName, UpdatedSSG) 
    end;

set_field_attributes(AvpList, _, _, SSG) -> {error, {invalid_argument, {AvpList, SSG}}}.

%-------------------------------------------------------------
%  
%-------------------------------------------------------------
-spec set_field_attribute(mb_field_attribute(), term(), mb_field_name(), mb_schema_name(), mb_ssg()) -> mb_ssg() | mb_error().
%-------------------------------------------------------------
set_field_attribute(Attribute, Value, FieldName, SchemaName, SSG) when is_map(SSG) ->

    % Screening ...

    ScreenResult = case validate_field_attribute(Attribute, Value, FieldName, SchemaName, SSG) of
        true -> 
            % Role field cannot be changed here, must call make_key.
            case Attribute /= ?ROLE of 
                true -> 
                    case (Attribute == ?PRIORITY andalso Value == optional) of 
                        true -> 
                            case key_name(SchemaName, SSG) of 
                                {error, Reason} -> {error, Reason};
                                FieldName -> {error, key_cannot_be_optional};
                                _ -> ok
                            end;
                        false -> ok
                    end;
                            
                _ -> {error, {not_permitted, Attribute}}
            end;

        false -> {error, {invalid_attribute_value_pair, {Attribute, Value}}}
    end,
        
    case ScreenResult of 
        ok ->
            case Attribute of 

                ?NAME -> rename_field(Value, FieldName, SchemaName, SSG);
                ?POSITION -> move_field(FieldName, Value, SchemaName, SSG);
                _ ->
                
                    Schemas = schemas(SSG),

                    case lists:keyfind(SchemaName, 1, Schemas) of 
                        {SchemaName, SchemaSpecifications} ->                         
                            case maps:find(?FIELDS, SchemaSpecifications) of

                                {ok, FieldList} ->
                                    case update_field(Attribute, Value, FieldName, FieldList) of
                                        {error, Reason1} -> {error, Reason1};
                                        UpdatedFieldList ->
                                            UpdatedSchemaSpecifications = maps:update(?FIELDS, UpdatedFieldList, SchemaSpecifications),
                                            UpdatedSchemas = lists:keyreplace(SchemaName, 1, Schemas, {SchemaName, UpdatedSchemaSpecifications}),
                                            UpdatedSSG = maps:update(?SCHEMAS, UpdatedSchemas, SSG),

                                            % If we made a mandatory field optional, the default value type must remain consistent.
                                            % If we changed the type of an optional field, the default value must remain consistent.
                                            % If the default value has a different type, select an appropriate default value.
                                            FieldPriority = get_field_priority(FieldName, SchemaName, UpdatedSSG),

                                            case (((Attribute == ?PRIORITY) andalso (Value == optional)) or 
                                                ((Attribute == ?FIELD_TYPE) andalso (FieldPriority == optional))) of 
                                                true ->                                                
                                                    case get_field_type(FieldName, SchemaName, UpdatedSSG) of 
                                                        {error, Reason2} -> {error, Reason2};
                                                        NewType ->  
                                                            case get_field_default_value(FieldName, SchemaName, UpdatedSSG) of 
                                                                {error, Reason2} -> {error, Reason2};
                                                                DefaultValue -> 
                                                                    case get_type(DefaultValue) of 
                                                                        NewType -> UpdatedSSG; % types are consistent
                                                                        _ -> % need to change the default value
                                                                            case NewType  of
                                                                                atom -> NewDefaultValue = not_defined;
                                                                                integer -> NewDefaultValue = 0;
                                                                                float -> NewDefaultValue = 0.0;
                                                                                string -> NewDefaultValue = "";
                                                                                list -> NewDefaultValue = [];
                                                                                tuple -> NewDefaultValue = {};
                                                                                map -> NewDefaultValue = #{};
                                                                                term -> NewDefaultValue = DefaultValue % term is a catch all
                                                                            end,

                                                                            % Update the latest SSG and return it
                                                                            set_field_attribute(?DEFAULT_VALUE, NewDefaultValue, FieldName, SchemaName, UpdatedSSG)
                                                                    end
                                                            end
                                                    end;
                                                false -> UpdatedSSG 
                                            end
                                    end;
                                error -> {error, {invalid_field_attribute, Attribute}}
                            end;
                        false -> {error, {schema_name_not_found, SchemaName}}
                    end
            end;
        Error -> Error
    end;

set_field_attribute(_, _, _, _, SSG) -> {error, {invalid_argument, SSG}}.


%-------------------------------------------------------------
%   
%-------------------------------------------------------------
-spec move_field(mb_field_name(), integer(), mb_schema_name(), mb_ssg()) -> mb_ssg() | mb_error().
%-------------------------------------------------------------
move_field(FieldName, ToPosition, SchemaName, SSG) when is_integer(ToPosition), ToPosition > 0, is_map(SSG) ->

    case ToPosition > 1 of 
        true -> move_any_field(FieldName, ToPosition, SchemaName, SSG);
        false -> {error, move_to_key_position}
    end;

move_field(_, ToPosition, _, SSG) -> {error, {invalid_argument, {ToPosition, SSG}}}.


%-------------------------------------------------------------
%   
%-------------------------------------------------------------
-spec make_key(mb_field_name(), mb_schema_name(), mb_ssg()) -> mb_ssg() | mb_error().
%-------------------------------------------------------------
make_key(FieldName, SchemaName, SSG) when is_map(SSG) ->

    case is_field(FieldName, SchemaName, SSG) of 
        {error, Reason} -> {error, Reason};
        true -> 
            case key_name(SchemaName, SSG) of 
                {error, Reason} -> {error, Reason};
                FieldName -> SSG; % Field is already key
                OldKey -> 
                    % Move the field to the first position and renumber
                    % all the fields.
                    case move_any_field(FieldName, 1, SchemaName, SSG) of 
                        {error, Reason} -> {error, Reason};
                        UpdatedSSG1 -> 
                            % Make the new key mandatory in case it is not.
                            case set_field_priority(mandatory, FieldName, SchemaName, UpdatedSSG1) of 
                                {error, Reason} -> {error, Reason};
                                UpdatedSSG2 ->
                                    % Now what is left is set the role of the new key to key
                                    % and the old key to field. 
                                    Schemas = schemas(UpdatedSSG2),

                                    case lists:keyfind(SchemaName, 1, Schemas) of % No escape clause, by now the specification must be valid
                                        {SchemaName, SchemaSpecifications} ->                         
                                            case maps:find(?FIELDS, SchemaSpecifications) of 

                                                {ok, FieldList} ->
                                                    case update_field(?ROLE, key, FieldName, FieldList) of
                                                        {error, Reason} -> {error, Reason};
                                                        UpdatedFieldList1 ->
                                                            case update_field(?ROLE, field, OldKey, UpdatedFieldList1) of 
                                                                {error, Reason} -> {error, Reason};
                                                                UpdatedFieldList2 -> update_schema_fields(UpdatedFieldList2, SchemaName, UpdatedSSG2)
                                                            end
                                                    end
                                            end
                                    end                                                
                            end
                    end
            end;
        false -> {error, {field_name_not_found, FieldName}}
    end;

make_key(_, _, SSG) -> {error, {invalid_argument, SSG}}.


%-------------------------------------------------------------
%   
%-------------------------------------------------------------
-spec set_field_name(mb_field_name(), mb_field_name(), mb_schema_name(), mb_ssg()) -> mb_ssg() | mb_error().
%-------------------------------------------------------------
set_field_name(NewName, OldName, SchemaName, SSG) when is_map(SSG) ->
    set_field_attribute(?NAME, NewName, OldName, SchemaName, SSG).


%-------------------------------------------------------------
%   
%-------------------------------------------------------------
-spec set_field_description(string(), mb_field_name(), mb_schema_name(), mb_ssg()) -> mb_ssg() | mb_error().
%-------------------------------------------------------------
set_field_description(Description, FieldName, SchemaName, SSG) when is_map(SSG) ->
    set_field_attribute(?DESCRIPTION, Description, FieldName, SchemaName, SSG).

%-------------------------------------------------------------
%   
%-------------------------------------------------------------
-spec set_field_label(string(), mb_field_name(), mb_schema_name(), mb_ssg()) -> mb_ssg() | mb_error().
%-------------------------------------------------------------
set_field_label(Label, FieldName, SchemaName, SSG) when is_map(SSG) ->
    set_field_attribute(?LABEL, Label, FieldName, SchemaName, SSG).

%-------------------------------------------------------------
%   
%-------------------------------------------------------------
-spec set_field_type(mb_value_type(), mb_field_name(), mb_schema_name(), mb_ssg()) -> mb_ssg() | mb_error().
%-------------------------------------------------------------
set_field_type(Type, FieldName, SchemaName, SSG) when is_map(SSG) ->
    set_field_attribute(?FIELD_TYPE, Type, FieldName, SchemaName, SSG).

%-------------------------------------------------------------
%   
%-------------------------------------------------------------
-spec set_field_priority(mb_field_priority(), mb_field_name(), mb_schema_name(), mb_ssg()) -> mb_ssg() | mb_error().
%-------------------------------------------------------------
set_field_priority(Priority, FieldName, SchemaName, SSG) when is_map(SSG) ->
    set_field_attribute(?PRIORITY, Priority, FieldName, SchemaName, SSG).
%-------------------------------------------------------------
%   
%-------------------------------------------------------------
-spec set_field_default_value(term(), mb_field_name(), mb_schema_name(), mb_ssg()) -> mb_ssg() | mb_error().
%-------------------------------------------------------------
set_field_default_value(DefaultValue, FieldName, SchemaName, SSG) when is_map(SSG) ->
    set_field_attribute(?DEFAULT_VALUE, DefaultValue, FieldName, SchemaName, SSG).



%-------------------------------------------------------------
%   
%-------------------------------------------------------------
-spec get_field_description(mb_field_name(), mb_schema_name(), mb_ssg()) -> mb_ssg() | mb_error().
%-------------------------------------------------------------
get_field_description(FieldName, SchemaName, SSG) when is_map(SSG) ->
    get_field_attribute(?DESCRIPTION, FieldName, SchemaName, SSG).

%-------------------------------------------------------------
%   
%-------------------------------------------------------------
-spec get_field_label(mb_field_name(), mb_schema_name(), mb_ssg()) -> mb_ssg() | mb_error().
%-------------------------------------------------------------
get_field_label(FieldName, SchemaName, SSG) when is_map(SSG) ->
    get_field_attribute(?LABEL, FieldName, SchemaName, SSG).

%-------------------------------------------------------------
%   
%-------------------------------------------------------------
-spec get_field_type(mb_field_name(), mb_schema_name(), mb_ssg()) -> mb_ssg() | mb_error().
%-------------------------------------------------------------
get_field_type(FieldName, SchemaName, SSG) when is_map(SSG) ->
    get_field_attribute(?FIELD_TYPE, FieldName, SchemaName, SSG).

%-------------------------------------------------------------
%   
%-------------------------------------------------------------
-spec get_field_priority(mb_field_name(), mb_schema_name(), mb_ssg()) -> mb_ssg() | mb_error().
%-------------------------------------------------------------
get_field_priority(FieldName, SchemaName, SSG) when is_map(SSG) ->
    get_field_attribute(?PRIORITY, FieldName, SchemaName, SSG).

%-------------------------------------------------------------
%   
%-------------------------------------------------------------
-spec get_field_default_value(mb_field_name(), mb_schema_name(), mb_ssg()) -> mb_ssg() | mb_error().
%-------------------------------------------------------------
get_field_default_value(FieldName, SchemaName, SSG) when is_map(SSG) ->
    get_field_attribute(?DEFAULT_VALUE, FieldName, SchemaName, SSG).

%-------------------------------------------------------------
%   
%-------------------------------------------------------------
-spec get_field_position(mb_field_name(), mb_schema_name(), mb_ssg()) -> mb_ssg() | mb_error().
%-------------------------------------------------------------
get_field_position(FieldName, SchemaName, SSG) when is_map(SSG) ->
    get_field_attribute(?POSITION, FieldName, SchemaName, SSG).

%-------------------------------------------------------------
%   
%-------------------------------------------------------------
-spec get_field_attribute(mb_field_attribute(), mb_field_name(), mb_schema_name(), mb_ssg()) -> term() | mb_error().
%-------------------------------------------------------------
get_field_attribute(Attribute, FieldName, SchemaName, SSG) when is_map(SSG) -> 

    case fields(SchemaName, SSG) of 
        {error, Reason} -> {error, Reason};
        
        Fields ->
            case lists:keyfind(FieldName, 1, Fields) of 
                {FieldName, FieldSpecifications} -> 
                    case is_field_attribute(Attribute) of 
                        true -> 
                            case maps:find(Attribute, FieldSpecifications) of
                                {ok, Value} -> Value;
                                error -> {error, {invalid_argument, SSG}}
                            end;

                        false -> {error, {invalid_argument, Attribute}}
                    end;

                false -> {error, {field_name_not_found, FieldName}}
            end
    end;

get_field_attribute(_, _, _, SSG) -> {error, {invalid_argument, SSG}}.


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec is_field(mb_field_name(), mb_schema_name(), mb_ssg()) -> boolean() | mb_error().
%-------------------------------------------------------------
is_field(FieldName, SchemaName, SSG) when is_map(SSG) ->

    case fields(SchemaName, SSG) of 
        {error, Reason} -> {error, Reason};
        Fields -> lists:keymember(FieldName, 1, Fields)
    end;

is_field(_, _, SSG) -> {error, {invalid_argument, SSG}}.


%-------------------------------------------------------------
%   
%------------------------------------------------------------- 
%-spec update_schema_fields(mb_field_spec_list(), mb_schema_name(), mb_ssg()) -> mb_ssg() | mb_error().
%------------------------------------------------------------- 
update_schema_fields(FieldList, SchemaName, SSG) when is_map(SSG) -> 

    %% First, validate the field list
    case is_field_list(FieldList) of 
        true -> 
            SchemasList = schemas(SSG),

            case lists:keyfind(SchemaName, 1, SchemasList) of

                {SchemaName, SchemaSpecifications} ->
                    UpdatedSchemaSpecifications = maps:update(?FIELDS, FieldList, SchemaSpecifications),

                    UpdatedSchemasList = lists:keyreplace(SchemaName, 1, SchemasList, {SchemaName, UpdatedSchemaSpecifications}),
                    maps:update(?SCHEMAS, UpdatedSchemasList, SSG);

                false -> {error, {schema_name_not_found, SchemaName}}
            end;

        false -> {error, {bad_field_list, FieldList}}
    end;

update_schema_fields(_, _, SSG) -> {error, {invalid_argument, SSG}}.

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec field_count(mb_schema_name(), mb_ssg()) -> integer() | mb_error().
%-------------------------------------------------------------
field_count(SchemaName, SSG) when is_map(SSG) ->

    case fields(SchemaName, SSG) of 
        {error, Reason} -> {error, Reason};
        Fields -> length(Fields) 
    end;

field_count(_, SSG) -> {error, {invalid_argument, SSG}}.


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec mandatory_field_count(mb_schema_name(), mb_ssg()) -> integer() | mb_error().
%-------------------------------------------------------------
mandatory_field_count(SchemaName, SSG) when is_map(SSG) ->

    case fields(SchemaName, SSG) of 
        {error, Reason} -> {error, Reason};
        Fields -> mandatory_field_count_next(Fields, 0)
    end;

mandatory_field_count(_, SSG) -> {error, {invalid_argument, SSG}}.


mandatory_field_count_next([], FinalMandatoryCount) -> FinalMandatoryCount;
mandatory_field_count_next([{_, FieldSpec} | T], MandatoryCount) ->
    
    case maps:find(?PRIORITY, FieldSpec) of 
        {ok, mandatory} -> mandatory_field_count_next(T, MandatoryCount+1);
        {ok, optional} -> mandatory_field_count_next(T, MandatoryCount);
        error -> {error, {invalid_argument, FieldSpec}}
    end.

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec field_names(mb_schema_name(), mb_ssg()) -> list() | mb_error().
%-------------------------------------------------------------
field_names(SchemaName, SSG) when is_map(SSG) -> 
    case fields(SchemaName, SSG) of 
        {error, Reason} -> {error, Reason};
        Fields -> field_names_next(Fields, [])
    end;

field_names(_, SSG) -> {error, {invalid_argument, SSG}}.


field_names_next([], FinalList) -> lists:reverse(FinalList);
field_names_next([{FieldName, _} | T], CompiledList) -> field_names_next(T, [FieldName | CompiledList]).


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec key_name(mb_schema_name(), mb_ssg()) -> mb_field_name() | mb_error().
%-------------------------------------------------------------
key_name(SchemaName, SSG) when is_map(SSG) -> 

    case fields(SchemaName, SSG) of 
        {error, Reason} -> {error, Reason};
        [] -> {error, fields_not_defined};
        [{Key, _}|_] -> Key
    end;

key_name(_, SSG) -> {error, {invalid_argument, SSG}}.


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec key_type(mb_schema_name(), mb_ssg()) -> mb_value_type() | mb_error().
%-------------------------------------------------------------
key_type(SchemaName, SSG) when is_map(SSG) -> 

    case field_names(SchemaName, SSG) of 
        {error, Reason} -> {error, Reason};
        [] -> {error, fields_not_defined};
         [Key|_] -> get_field_attribute(type, Key, SchemaName, SSG)
    end;

key_type(_, SSG) -> {error, {invalid_argument, SSG}}.

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec field_position(mb_field_name(), mb_schema_name(), mb_ssg()) -> integer() | mb_error().
%-------------------------------------------------------------
field_position(FieldName, SchemaName, SSG) -> get_field_attribute(?POSITION, FieldName, SchemaName, SSG).

%============================================================
%    CODE GENERATION APIs
%============================================================

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
%-------------------------------------------------------------
generate(Module, SSG) -> generate(Module, ".", ".", SSG).

%-------------------------------------------------------------
%   
%-------------------------------------------------------------
%-------------------------------------------------------------
generate(Module, SrcPath, HrlPath, SSG) when is_atom(Module), is_list(SrcPath), is_list(HrlPath), is_map(SSG) ->

    io:format("generate::started~n"),

    case mb_utilities:is_unquoted_atom(Module) of 

        
        true ->            
            case file:open(HrlPath ++ "/" ++ atom_to_list(Module) ++ ".hrl", [write]) of 
                {ok, HrlIoDevice} ->

                    io:format("generate::header file opened for writing~n"),
                    
                    generate_records(SSG, HrlIoDevice),

                    case file:open(SrcPath ++ "/" ++ atom_to_list(Module) ++ ".erl", [write]) of
                        {ok, SrcIoDevice}  -> 
                            
                            io:format("generate::source file opened for writing~n"),
                            
                            io:format(SrcIoDevice, "-module(~p).~n", [Module]),

                            

                            io:format(SrcIoDevice, "~n", []),
                            io:format(SrcIoDevice, "-export([get_ssg/0]).~n", []),

                            io:format(SrcIoDevice, "-export([install/0, install/1, start/0, stop/0, table_size/1, table_sizes/0]).~n", []),
                            io:format(SrcIoDevice, "-export([schema_names/0, is_schema/1, is_field/2, schemas/0, get_schema/1, get_schema_attribute/2]).~n", []),
                            io:format(SrcIoDevice, "-export([fields/1, field_count/1, mandatory_field_count/1, field_names/1, key_name/1, key_type/1, field_position/2, get_field_attribute/3]).~n", []),
                            io:format(SrcIoDevice, "-export([read/2, select/4, select_or/6, select_and/6, build_matchhead/1]).~n", []),
                            io:format(SrcIoDevice, "-export([add/1, add/2, add/3, delete/1, delete/2, clear_all_tables/0]).~n", []),
                            io:format(SrcIoDevice, "-export([build_schema_record_from_specifications/1, convert_schema_data_avp_list_into_record_tuple/1]).~n", []),
                            io:format(SrcIoDevice, "~n", []),
                            io:format(SrcIoDevice, "get_ssg() ->~n", []),
                            io:format(SrcIoDevice, "    ~p.~n", [SSG]),    

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
                            
                            file:close(SrcIoDevice),
                            ok;

                        {error, Reason} -> {error, Reason}
                    end;

                {error, Reason} -> {error, Reason}
            end;

        false -> {error, {invalid_module_name, Module}}
    end;

generate(Module, SrcPath, HrlPath, SSG) -> 
    io:format("mb_schemas::error, match guard failed~n"),
    {error, {invalid_argument, {Module, SrcPath, HrlPath, SSG}}}.


%============================================================
%    UTILITY APIs
%============================================================

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
%-------------------------------------------------------------
convert_from_stirng(Value, Type) ->

  case is_list(Value) of
    true ->
      case Type of
        integer -> {integer, mb_utilities:string_to_integer(Value)};
        float -> {float, mb_utilities:string_to_float(Value)};
        string -> {string, Value};
        list -> {list, Value};
        atom -> {atom, list_to_atom(Value)};
        tuple -> 
            case mb_utilities:string_to_tuple(Value) of 
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
%-------------------------------------------------------------
convert_schema_data_avp_list_into_record_tuple(AvpList, SSG) when is_list(AvpList), is_map(SSG) -> 
        
    case lists:keyfind(schema, 1, AvpList) of
        {schema, SchemaName} -> 
            % Get the ordered list of fields, [{FieldName, FieldSpecMap}]
            case fields(SchemaName, SSG) of 
                {error, Reason} -> {error, Reason};
                Fields ->
                    case validate_schema_data_avp_list_next(Fields, lists:keydelete(schema, 1, AvpList), [{schema, SchemaName}]) of
                        {error, Reason} -> {error, Reason};
                        ValidatedList -> schema_data_avp_list_to_record_tuple(ValidatedList) 
                    end
            end;

        false -> {error, schema_name_not_in_avp}
    end;

convert_schema_data_avp_list_into_record_tuple(AvpList, SSG) -> {error, {invalid_argument, {AvpList, SSG}}}.


validate_schema_data_avp_list_next([], [], FinalList) -> FinalList;
validate_schema_data_avp_list_next([],_, _) -> {error, invalid_fields};
validate_schema_data_avp_list_next([{FieldName, FieldSpecMap} | F], AvpList, InterimList) ->

    case lists:keyfind(FieldName, 1, AvpList) of 

        {FieldName, FieldValue} -> 
            % Found it, the field is in the supplied list. Validate that
            % the supplied value has the right type.
            case maps:find(type, FieldSpecMap) of 
                {ok, ExpectedType} ->
                    ActualType = get_type(FieldValue),
                    case ExpectedType == ActualType of
                        true -> validate_schema_data_avp_list_next(F, lists:keydelete(FieldName, 1, AvpList), [{FieldName, FieldValue} | InterimList]);
                        false -> {error, {invalid_type, {FieldName, FieldValue, ActualType}}}
                    end;
                error -> {error, {invalid_argument, FieldSpecMap}}
            end;

        false ->
            % If mandatory, then validation fails, otherwise
            % we will select the default value.

            case maps:find(priority, FieldSpecMap) of
                {ok, mandatory} -> {error, {missing_mandatory_field, FieldName}};
                {ok, optional} -> validate_schema_data_avp_list_next(F, lists:keydelete(FieldName, 1, AvpList), [{FieldName, maps:get(default_value, FieldSpecMap)} | InterimList]);
                error -> {error, {invalid_argument, FieldSpecMap}}
            end 
    end.

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
%-------------------------------------------------------------
schema_data_avp_list_to_record_tuple(SchemaDataAvpList) -> 
    schema_data_avp_list_to_record_tuple_next(SchemaDataAvpList, []).

schema_data_avp_list_to_record_tuple_next([], FinalList) -> list_to_tuple(FinalList);
schema_data_avp_list_to_record_tuple_next([{_, Value} | T], InterimList) -> schema_data_avp_list_to_record_tuple_next(T, [Value | InterimList]);
schema_data_avp_list_to_record_tuple_next(InvalidList, _) -> {error, {invalid_argument, InvalidList}}.
%-------------------------------------------------------------
% 
%-------------------------------------------------------------
%-------------------------------------------------------------
build_schema_record_from_specifications(SchemaName, SSG) when is_map(SSG) -> 
    tuple_to_list([SchemaName, field_names(SchemaName, SSG)]);

build_schema_record_from_specifications(_, SSG) -> {error, {invalid_argument, SSG}}.

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
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
% 
%-------------------------------------------------------------
%-------------------------------------------------------------
create_schema(SchemaName) when is_atom(SchemaName) -> 

    maps:put(?DESCRIPTION, [], 
        maps:put(?FIELDS, [], 
            maps:put(?RAM_COPIES, [], 
                maps:put(?DISC_ONLY_COPIES, [], 
                    maps:put(?DISC_COPIES, [], 
                        maps:put(?SCHEMA_TYPE, set, 
                            maps:put(?NAME, SchemaName, maps:new())))))));

create_schema(SchemaName) -> {error, {invalid_argument, SchemaName}}.

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
%-------------------------------------------------------------
create_field(FieldName) when is_atom(FieldName) ->

  maps:put(?DESCRIPTION, "", 
    maps:put(?DEFAULT_VALUE, not_defined, 
        maps:put(?PRIORITY, mandatory, 
            maps:put(?POSITION, 0, 
                maps:put(?SCHEMA_TYPE, term, 
                    maps:put(?ROLE, field, 
                        maps:put(?LABEL, "", 
                            maps:put(?NAME, FieldName, maps:new()))))))));

create_field(SchemaName) -> {error, {invalid_argument, SchemaName}}.

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
%-------------------------------------------------------------
add_field(FieldName, FieldList) when is_atom(FieldName), is_list(FieldList) ->

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
% Sets field attributes, does not do any validation.
%-------------------------------------------------------------
%-------------------------------------------------------------
update_field(Attribute, Value, FieldName, FieldList) when is_list(FieldList) ->

    case lists:keyfind(FieldName, 1, FieldList) of 
        {FieldName, FieldSpecifications} ->
            UpdatedFieldSpecifications = maps:update(Attribute, Value, FieldSpecifications),

            case Attribute == ?NAME of 
                true -> UpdatedFieldName = Value;
                false -> UpdatedFieldName = FieldName 
            end,

            lists:keyreplace(FieldName, 1, FieldList, {UpdatedFieldName, UpdatedFieldSpecifications});

        false -> {error, {field_name_not_found, {FieldName, FieldList}}}
    end;

update_field(_, _, _, FieldList) -> {error, {invalid_argument, FieldList}}.

      
%-------------------------------------------------------------
% 
%------------------------------------------------------------- 
%-------------------------------------------------------------
is_field_list([]) -> true;
is_field_list([{FieldName, FieldSpecifications} | T]) when is_map(FieldSpecifications) ->

    case maps:find(?NAME, FieldSpecifications) of 
        {ok, FieldName} -> 
            is_field_list(T);
        _ -> 
            false 
    end; 
is_field_list(_) -> false.

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
generate_function(FunctionName, BaseModule, IoDevice) ->
    io:format(IoDevice, "~n~p() -> ~p:~p().~n", 
        [FunctionName, BaseModule, FunctionName]).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
%generate_function(FunctionName, Arg1, BaseModule, IoDevice) ->
%    io:format(IoDevice, "~n~p(~s) -> ~p:~p(~s).~n", 
%        [FunctionName, Arg1, BaseModule, FunctionName, Arg1]).

%-------------------------------------------------------------
%   
%-------------------------------------------------------------
generate_function(FunctionName, Arg1, Arg2, BaseModule, IoDevice) ->
    io:format(IoDevice, "~n~p(~s, ~s) -> ~p:~p(~s, ~s).~n", 
        [FunctionName, Arg1, Arg2, BaseModule, FunctionName, Arg1, Arg2]).

%-------------------------------------------------------------
%   
%-------------------------------------------------------------
%generate_function(FunctionName, Arg1, Arg2, Arg3, BaseModule, IoDevice) ->
%    io:format(IoDevice, "~n~p(~s, ~s, ~s) -> ~p:~p(~s, ~s, ~s).~n", 
%        [FunctionName, Arg1, Arg2, Arg3, BaseModule, FunctionName, Arg1, Arg2, Arg3]).

%-------------------------------------------------------------
%  
%-------------------------------------------------------------
generate_spec_function(FunctionName, BaseModule, IoDevice) -> 
    io:format(IoDevice, "~n~p() -> ~p:~p(get_ssg()).~n", 
        [FunctionName, BaseModule, FunctionName]).


%-------------------------------------------------------------
%   
%-------------------------------------------------------------
generate_spec_function(FunctionName, Arg1, BaseModule, IoDevice) -> 
    io:format(IoDevice, "~n~p(~s) -> ~p:~p(~s, get_ssg()).~n", 
        [FunctionName, Arg1, BaseModule, FunctionName, Arg1]).

%-------------------------------------------------------------
%  
%-------------------------------------------------------------
generate_spec_function(FunctionName, Arg1, Arg2, BaseModule, IoDevice) -> 
    io:format(IoDevice, "~n~p(~s, ~s) -> ~p:~p(~s, ~s, get_ssg()).~n", 
        [FunctionName, Arg1, Arg2, BaseModule, FunctionName, Arg1, Arg2]).


%-------------------------------------------------------------
%   
%-------------------------------------------------------------
generate_spec_function(FunctionName, Arg1, Arg2, Arg3, BaseModule, IoDevice) -> 
    io:format(IoDevice, "~n~p(~s, ~s, ~s) -> ~p:~p(~s, ~s, ~s, get_ssg()).~n", 
        [FunctionName, Arg1, Arg2, Arg3, BaseModule, FunctionName, Arg1, Arg2, Arg3]).


%-------------------------------------------------------------
%  
%-------------------------------------------------------------
generate_spec_function(FunctionName, Arg1, Arg2, Arg3, Arg4, BaseModule, IoDevice) -> 
    io:format(IoDevice, "~n~p(~s, ~s, ~s, ~s) -> ~p:~p(~s, ~s, ~s, ~s, get_ssg()).~n", 
        [FunctionName, Arg1, Arg2, Arg3, Arg4, BaseModule, FunctionName, Arg1, Arg2, Arg3, Arg4]).


%-------------------------------------------------------------
%  
%-------------------------------------------------------------
generate_spec_function(FunctionName, Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, BaseModule, IoDevice) -> 
    io:format(IoDevice, "~n~p(~s, ~s, ~s, ~s, ~s, ~s) -> ~p:~p(~s, ~s, ~s, ~s, ~s, ~s, get_ssg()).~n", 
        [FunctionName, Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, 
         BaseModule, FunctionName, Arg1, Arg2, Arg3, Arg4, Arg5, Arg6]).


%-------------------------------------------------------------
%   
%-------------------------------------------------------------
generate_records(SSG, IoDevice) when is_map(SSG) -> 

    case maps:find(?SCHEMAS, SSG) of 
        {ok, Schemas} -> generate_record(Schemas, IoDevice);
        error -> {error, {invalid_ssg_format, SSG}} 
    end;

    generate_records(SSG, _IoDevice) -> {error, {invalid_argument, SSG}}.


%-------------------------------------------------------------
%   
%-------------------------------------------------------------
generate_record([], _) -> ok;
generate_record([{SchemaName, SchemaSpecifications} | T], HrlIoDevice) -> 

    case maps:find(?FIELDS, SchemaSpecifications) of 
        {ok, FieldList} ->
            io:format(HrlIoDevice, "-record(~p, {", [SchemaName]),
            generate_record_field(FieldList, HrlIoDevice),
            io:format(HrlIoDevice, "}).~n", []),
            generate_record(T, HrlIoDevice);

        error -> {error, {invalid_argument, SchemaSpecifications}}
    end.

%-------------------------------------------------------------
% 
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
%   
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
    map -> true;
    _ -> false 
  end.

%-------------------------------------------------------------
%   
%-------------------------------------------------------------
-spec get_type(term()) -> atom().
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

              case mb_utilities:is_printable_string(Value) of
                true -> string;
                false -> list
              end;

            false -> 

              case is_atom(Value) of
                true -> atom;
                false -> 

                  case is_tuple(Value) of 
                    true -> tuple;
                    false -> 
                        
                        case is_map(Value) of 
                            true -> map;
                            false -> term
                        end
                  end
              end 
          end 
      end 
  end.


%-------------------------------------------------------------
%   
%-------------------------------------------------------------
-spec rename_schema(mb_schema_name(), mb_schema_name(), mb_ssg()) -> mb_ssg() | mb_error().
%-------------------------------------------------------------
rename_schema(NewName, OldName, SSG) ->
    % Is the new name existing?
    case is_schema(NewName, SSG) of 
        false -> 
            % Fetch the old schema
            case schemas(SSG) of 
                {error, Reason} -> {error, Reason};
                
                Schemas ->
                    case lists:keyfind(OldName, 1, Schemas) of 
                        {OldName, Specifications} -> 
                            % Delete the old schema from the schemas list
                            ReducedSchemas = lists:keydelete(OldName, 1, Schemas),

                            % Update the name in the schema specifications instance
                            UpdatedSpecifications = maps:update(?NAME, NewName, Specifications),

                            % Write the specifications with the updated name to the schema list
                            UpdatedSchemas = [{NewName, UpdatedSpecifications} | ReducedSchemas],

                            % Put back the updated schemas list in the SSG
                            maps:update(?SCHEMAS, UpdatedSchemas, SSG);

                        false -> {error, {schema_name_not_found, OldName}}
                    end
            end;

        true -> {error, {schema_name_exists, NewName}};

        {error, Reason} -> {error, Reason}
    end.

%-------------------------------------------------------------
%   
%-------------------------------------------------------------
rename_field(NewName, OldName, SchemaName, SSG) ->

    case is_field(NewName, SchemaName, SSG) of 
        false -> 
            case is_field(OldName, SchemaName, SSG) of 
                true ->
                    Schemas = schemas(SSG),

                    case lists:keyfind(SchemaName, 1, Schemas) of 
                        {SchemaName, SchemaSpecifications} ->
                                
                            case maps:find(?FIELDS, SchemaSpecifications) of 
                                {ok, FieldList} ->

                                    case update_field(?NAME, NewName, OldName, FieldList) of 
                                        {error, Error} -> {error, Error};
                                        UpdatedFieldList ->
                                            UpdatedSchemaSpecifications = maps:update(?FIELDS, UpdatedFieldList, SchemaSpecifications),
                                            UpdatedSchemas = lists:keyreplace(SchemaName, 1, Schemas, {SchemaName, UpdatedSchemaSpecifications}),
                                            maps:update(?SCHEMAS, UpdatedSchemas, SSG)
                                    end;
                                error -> {error, {invalid_specifications, SSG}}
                            end;
                        false -> {error, {schema_name_not_found, SchemaName}}
                    end;
                false -> {error, {field_not_found, OldName}}
            end;
        true -> {error, {field_exists, NewName}}
    end.

%-------------------------------------------------------------
%   
%-------------------------------------------------------------
-spec move_any_field(mb_field_name(), integer(), mb_schema_name(), mb_ssg()) -> mb_ssg() | mb_error().
%-------------------------------------------------------------
move_any_field(FieldName, ToPosition, SchemaName, SSG) when is_integer(ToPosition), ToPosition > 0, is_map(SSG) ->

    Schemas = schemas(SSG),

    case lists:keyfind(SchemaName, 1, Schemas) of

        {SchemaName, SchemaSpecifications} -> 
            case maps:find(?FIELDS, SchemaSpecifications) of

                {ok, FieldList} ->

                    FieldCount = length(FieldList),

                    case ToPosition =< FieldCount of 
                        true ->
                            case lists:keyfind(FieldName, 1, FieldList) of 
                                {FieldName, FieldSpecifications} -> 

                                    CurrentPosition = maps:get(?POSITION, FieldSpecifications),
                                    UpdatedFieldList1 = mb_utilities:move_element(FieldList, CurrentPosition, ToPosition),
                                    UpdatedFieldList2 = update_field_positions(UpdatedFieldList1),
                                    update_schema_fields(UpdatedFieldList2, SchemaName, SSG);
                                false -> {error, {field_name_not_found, FieldName}}
                            end;
                        false -> {error, {position_greater_than_field_count, ToPosition}}
                    end;

                _ -> {error, {invalid_specifications, SSG}}
            end;
        false -> {error, {schema_name_not_found, SchemaName}}
    end;    

move_any_field(_FieldName, ToPosition, _SchemaName, SSG) -> {error, {invalid_argument, {ToPosition, SSG}}}.
                        
%-------------------------------------------------------------
%   
%-------------------------------------------------------------
-spec update_schema_attribute(mb_schema_attribute(), term(), mb_schema_name(), mb_schema_spec(), mb_ssg()) -> mb_ssg().
%-------------------------------------------------------------
update_schema_attribute(Attribute, Value, SchemaName, Schema, SSG) ->
    UpdatedSchemaSpecifications = maps:update(Attribute, Value, Schema),
    SchemasList = schemas(SSG),
    UpdatedSchemasList = lists:keyreplace(SchemaName, 1, SchemasList, {SchemaName, UpdatedSchemaSpecifications}),
    maps:update(?SCHEMAS, UpdatedSchemasList, SSG).


%-------------------------------------------------------------
%   
%-------------------------------------------------------------
-spec update_field_positions(mb_field_spec_list()) -> mb_field_spec_list().
%-------------------------------------------------------------
update_field_positions([]) -> [];
update_field_positions(FieldList) ->
    update_field_positions(FieldList, 1, []).


update_field_positions([], _, UpdatedFieldList) -> lists:reverse(UpdatedFieldList);
update_field_positions([{FieldName, FieldSpecifications}| Remainder], NextPos, Aggregate) ->

    UpdatedFieldSpecifications = maps:update(?POSITION, NextPos, FieldSpecifications),
    update_field_positions(Remainder, NextPos+1, [{FieldName, UpdatedFieldSpecifications} | Aggregate]).



%============================================================
%    TEST and PROTOTYPE FUNCTIONS
%============================================================



