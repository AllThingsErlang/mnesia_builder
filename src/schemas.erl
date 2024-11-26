-module(schemas).
-include("../include/schemas.hrl").
-export([get_tables/0, get_key_type/1, get_field_type/2, find_field_type/2, get_field_types/1,is_field/2, 
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
get_field_type(Table, FieldName) -> maps:get(FieldName, get_field_types(Table)).

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
        field_2_3=>list,
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
