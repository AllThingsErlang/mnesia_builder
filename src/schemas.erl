-module(schemas).
-include("../include/schemas.hrl").

-define(GEN_H_FILE_NAME, "test.hrl").

-export([schema_specifictions/0, schema_step/2, generate/0, generate/1, get_tables/0, get_key_type/1, get_field_type/2, find_field_type/2, get_field_types/1,is_field/2, 
         convert_from_string/2, safe_convert_from_string/2, validate_type/3, build_record/3]).



%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns:  
%-------------------------------------------------------------
get_tables() -> [table_1, table_2].

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns:  
%-------------------------------------------------------------
get_field_type(Table, FieldName) -> 
  
  Specs = schema_specifictions(),

  maps:get(FieldName, get_field_types(Table)).

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns:  
%-------------------------------------------------------------
find_field_type(Table, FieldName) -> maps:find(FieldName, get_field_types(Table)).

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns:  
%-------------------------------------------------------------
is_field(Table, FieldName) -> maps:is_key(FieldName, get_field_types(Table)).

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns:  
%-------------------------------------------------------------
get_key_type(Table) -> 

  case Table of
    table_1 -> get_field_type(Table, key_1);
    table_2 -> get_field_type(Table, key_2)
  end.

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns:  
%-------------------------------------------------------------
generate() -> generate("../include/" ++ ?GEN_H_FILE_NAME).

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns:  
%-------------------------------------------------------------
generate(FilePath) ->
    case file:open(FilePath, [write]) of
        {ok, IoDevice}  -> 
          schema_step(IoDevice, schema_specifictions()),
          file:close(IoDevice);
        
        {error, Reason} -> {error, Reason}
    end.

%-------------------------------------------------------------
% Function: 
% Purpose:   Steps through all the schemas in the schema spec map
%            and generates a record for each schema
% Returns:  
%-------------------------------------------------------------
schema_step(IoDevice, Map) ->
  Size = maps:size(Map),

  if 
    Size > 0 -> 
      schema_step(IoDevice, Map, 1);
    true -> ok
  end.

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns:  
%-------------------------------------------------------------
schema_step(IoDevice, Map, SchemaID) ->

  schema_parse(IoDevice, maps:get(SchemaID, Map)),
  
  Size = maps:size(Map),

  if 
    SchemaID < Size -> schema_step(IoDevice, Map, SchemaID+1);
    true -> ok
  end.

%-------------------------------------------------------------
% Function: 
% Purpose:  Parses the next schema and generates the record
% Returns:  
%-------------------------------------------------------------
schema_parse(IoDevice, Map) ->

  Size = maps:size(Map),

  if 

    Size > 1 ->
      SchemaName = maps:get(schema_name, Map),
      gen_record_start(IoDevice, SchemaName),

      SchemaSpecifications = maps:get(fields, Map),
      KeySpecifications = maps:get(1, SchemaSpecifications),

      KeyName = maps:get(field_name, KeySpecifications),
      gen_record_key(IoDevice, KeyName),

      schema_parse(IoDevice, SchemaSpecifications, 2);

    true -> io:format("invalid schema specifications~n")
  end.

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns:  
%-------------------------------------------------------------
schema_parse(IoDevice, Map, FieldID) ->

    gen_record_field_seperator(IoDevice),

    FieldSpecifications = maps:get(FieldID, Map),

    Name = maps:get(field_name, FieldSpecifications),
    Type = maps:get(type, FieldSpecifications),
    Priority = maps:get(priority, FieldSpecifications),

    if 
      Priority == optional -> 
        DefaultValue = maps:get(default_value, FieldSpecifications),
        gen_record_field(IoDevice, Name, DefaultValue, Type);

      true -> gen_record_field(IoDevice, Name)
    end,

    Size = maps:size(Map),

    if 
      FieldID < Size -> schema_parse(IoDevice, Map, FieldID+1);
      true -> gen_record_end(IoDevice)
    end.


%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns:  
%-------------------------------------------------------------
gen_record_start(IoDevice, RecordName) -> io:format(IoDevice, "-record(~s, {", [RecordName]).
gen_record_end(IoDevice) -> io:format(IoDevice, "}).~n", []).

gen_record_key(IoDevice, KeyName) -> io:format(IoDevice, "~p, ", [KeyName]).

gen_record_field(IoDevice, FieldName) -> io:format(IoDevice, "~p", [FieldName]).
gen_record_field_seperator(IoDevice) -> io:format(IoDevice, ", ", []).

gen_record_field(IoDevice, FieldName, DefaultValue, ValueType) ->
  io:format(IoDevice, "~p=", [FieldName]),

  case ValueType of

    string  -> io:format(IoDevice, "\"~s\"", [DefaultValue]);
    integer -> io:format(IoDevice, "~B", [DefaultValue]);
    float   -> io:format(IoDevice, "~f", [DefaultValue]);
    atom    -> io:format(IoDevice, "~p", [DefaultValue])

  end. 

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns:  
%-------------------------------------------------------------
schema_specifictions() ->

  #{1=>#{schema_name=>table_1,
         fields=>
          #{1=>#{field_name=>employee_id,
                label=>"Employee ID",
                role=>key,
                type=>string,
                description=>"table_1 key"},
                
            2=>#{field_name=>job_id,
                label=>"Job ID",
                role=>field,
                type=>integer,
                priority=>mandatory,
                description=>"table_1 field 2"},
                
            3=>#{field_name=>hourly_wage,
                label=>"Hourly Wage",
                role=>field,
                type=>float,
                priority=>mandatory,
                description=>"table_1 field 3"},
                
            4=>#{field_name=>office_id,
                label=>"Office Bld ID",
                role=>field,
                type=>string,
                priority=>optional,
                default_value=>"not defined",
                description=>"table_1 field 4"}}},

    2=>#{schema_name=>table_2,
         fields=>
          #{1=>#{field_name=>job_id,
                label=>"Job ID",
                role=>key,
                type=>integer,
                priority=>mandatory,
                description=>"Each job type has its own ID"},
                
            2=>#{field_name=>max_job_class,
                label=>"Max Job Class",
                role=>field,
                type=>integer,
                priority=>mandatory,
                description=>"Highest job classification qualified to take this job ID"},
            
            3=>#{field_name=>max_horly_wage,
                label=>"Max Hourly Wage",
                role=>field,
                type=>float,
                priority=>optional,
                default_value=>0.0,
                description=>"Highest hourly wage for this job ID"},
            
            4=>#{field_name=>admin_first_name,
                label=>"Admin First Name",
                role=>field,
                type=>string,
                priority=>optional,
                default_value=>"not defined",
                description=>"First name of admin that defined the job ID"},
            
            5=>#{field_name=>admin_last_name,
                label=>"Admin Last Name",
                role=>field,
                type=>string,
                priority=>optional,
                default_value=>"not defined",
                description=>"Last name of admin that defined the job ID"},
            
            6=>#{field_name=>status,
                label=>"Status",
                role=>field,
                type=>atom,
                priority=>mandatory,
                description=>"Job ID status"}}}
    }.

%-------------------------------------------------------------
% Function: 
% Purpose:  OBSOLETE
% Returns:  
%-------------------------------------------------------------
get_field_types(Table) -> 

  case Table of

    table_1 ->
    
      #{key_1=>string, 
        field_1_1=>integer,
        field_1_2=>float,
        field_1_3=>string};

    table_2 ->

      #{key_2=>integer, 
        field_2_1=>integer,
        field_2_2=>float,
        field_2_3=>string,
        field_2_4=>string,
        field_2_5=>atom}
  end.

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns:  
%-------------------------------------------------------------
validate_type(Table, FieldName, Value) ->

  Type = get_field_type(Table, FieldName),
  case Type of
    integer -> is_integer(Value);
    float -> is_float(Value);
    string -> is_list(Value);
    atom -> is_atom(Value);
    tuple -> is_tuple(Value);
    term -> true
  end.


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
build_record(Table, Key, Data) ->

  case Table of
        
    table_1 ->
        
      {Field1, Field2, Field3} = Data,
      
      Validation = schemas:validate_type(Table, key_1, Key) and
                    schemas:validate_type(Table, field_1_1, Field1) and
                    schemas:validate_type(Table, field_1_2, Field2) and
                    schemas:validate_type(Table, field_1_3, Field3),

      case Validation of
          true ->
              Record = #table_1{key_1 = Key, 
                  field_1_1 = Field1, 
                  field_1_2 = Field2, 
                  field_1_3 = Field3},

                  {ok, Record};

          false -> {error, invalid_type}

      end;

    table_2 ->
        
      {Field1, Field2, Field3, Field4, Field5} = Data,

      Validation = schemas:validate_type(Table, key_2, Key) and
                    schemas:validate_type(Table, field_2_1, Field1) and
                    schemas:validate_type(Table, field_2_2, Field2) and
                    schemas:validate_type(Table, field_2_3, Field3) and
                    schemas:validate_type(Table, field_2_4, Field4) and
                    schemas:validate_type(Table, field_2_5, Field5),

      case Validation of
          true ->
              Record = #table_2{key_2 = Key, 
                  field_2_1 = Field1, 
                  field_2_2 = Field2, 
                  field_2_3 = Field3,
                  field_2_4 = Field4,
                  field_2_5 = Field5},

              {ok, Record};

          false -> {error, invalid_type}
      end
  end.
