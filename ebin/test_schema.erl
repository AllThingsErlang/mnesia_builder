-module(test_schema).

-export([schema_specifications/0]).
-export([install/0, install/1, start/0, stop/0, table_size/1, table_sizes/0]).
-export([schema_names/0, is_schema/1, is_field/2, schemas/0, get_schema/1, get_schema_attribute/2]).
-export([fields/1, field_count/1, mandatory_field_count/1, field_names/1, key_name/1, key_type/1, field_position/2, get_field_attribute/3]).
-export([read/2, select/4, select_or/6, select_and/6, build_matchhead/1]).
-export([add/1, add/2, add/3, delete/1, delete/2, clear_all_tables/0]).
-export([build_schema_record_from_specifications/1, convert_schema_data_avp_list_into_record_tuple/1]).

schema_specifications() ->
    #{version => "0.2",
      schemas =>
          [{employees,
               #{name => employees,type => set,
                 fields =>
                     [{employee_id,
                          #{label => "Employee ID",name => employee_id,
                            position => 1,priority => mandatory,
                            type => integer,description => [],role => key,
                            default_value => not_defined}},
                      {employee_last_name,
                          #{label => [],name => employee_last_name,
                            position => 2,priority => mandatory,
                            type => string,description => [],role => field,
                            default_value => not_defined}},
                      {employee_first_name,
                          #{label => "First Name",name => employee_first_name,
                            position => 3,priority => optional,type => string,
                            description => [],role => field,
                            default_value => []}}],
                 disc_copies => [],disc_only_copies => [],ram_copies => []}},
           {departments,
               #{name => departments,type => set,
                 fields =>
                     [{department_id,
                          #{label => [],name => department_id,position => 1,
                            priority => mandatory,type => string,
                            description => [],role => key,
                            default_value => not_defined}},
                      {manager_last_name,
                          #{label => [],name => manager_last_name,
                            position => 2,priority => mandatory,
                            type => string,description => [],role => field,
                            default_value => not_defined}},
                      {manager_first_name,
                          #{label => [],name => manager_first_name,
                            position => 3,priority => mandatory,
                            type => string,description => [],role => field,
                            default_value => not_defined}}],
                 disc_copies => [],disc_only_copies => [],ram_copies => []}}]}.


%-------------------------------------------------------
%                DB Management Functions
%-------------------------------------------------------

install() -> db_management:install(schema_specifications()).

install(NodeList) -> db_management:install(NodeList, schema_specifications()).

start() -> db_management:start(schema_specifications()).

stop() -> db_management:stop().

table_size(SchemaName) -> db_management:table_size(SchemaName, schema_specifications()).

table_sizes() -> db_management:table_sizes(schema_specifications()).


%-------------------------------------------------------
%                     Schema Functions
%-------------------------------------------------------

schema_names() -> db_schemas:schema_names(schema_specifications()).

is_schema(SchemaName) -> db_schemas:is_schema(SchemaName, schema_specifications()).

is_field(FieldName, SchemaName) -> db_schemas:is_field(FieldName, SchemaName, schema_specifications()).

schemas() -> db_schemas:schemas(schema_specifications()).

get_schema(SchemaName) -> db_schemas:get_schema(SchemaName, schema_specifications()).

get_schema_attribute(Attribute, SchemaName) -> db_schemas:get_schema_attribute(Attribute, SchemaName, schema_specifications()).

fields(SchemaName) -> db_schemas:fields(SchemaName, schema_specifications()).

field_count(SchemaName) -> db_schemas:field_count(SchemaName, schema_specifications()).

mandatory_field_count(SchemaName) -> db_schemas:mandatory_field_count(SchemaName, schema_specifications()).

field_names(SchemaName) -> db_schemas:field_names(SchemaName, schema_specifications()).

key_name(SchemaName) -> db_schemas:key_name(SchemaName, schema_specifications()).

key_type(SchemaName) -> db_schemas:key_type(SchemaName, schema_specifications()).

field_position(FieldName, SchemaName) -> db_schemas:field_position(FieldName, SchemaName, schema_specifications()).

get_field_attribute(Attribute, FieldName, SchemaName) -> db_schemas:get_field_attribute(Attribute, FieldName, SchemaName, schema_specifications()).


%-------------------------------------------------------
%                     Query Functions
%-------------------------------------------------------

read(SchemaName, Key) -> db_query:read(SchemaName, Key).

select(SchemaName, Field, Oper, Value) -> db_query:select(SchemaName, Field, Oper, Value, schema_specifications()).

select_or(SchemaName, Field, Oper1, Value1, Oper2, Value2) -> db_query:select_or(SchemaName, Field, Oper1, Value1, Oper2, Value2, schema_specifications()).

select_and(SchemaName, Field, Oper1, Value1, Oper2, Value2) -> db_query:select_and(SchemaName, Field, Oper1, Value1, Oper2, Value2, schema_specifications()).

build_matchhead(SchemaName) -> db_query:build_matchhead(SchemaName, schema_specifications()).


%-------------------------------------------------------
%                     Modify Functions
%-------------------------------------------------------

add(Record) -> db_edit:add(Record, schema_specifications()).

add(SchemaName, Record) -> db_edit:add(SchemaName, Record, schema_specifications()).

add(SchemaName, Key, Data) -> db_edit:add(SchemaName, Key, Data, schema_specifications()).

delete(TableKey) -> db_edit:delete(TableKey, schema_specifications()).

delete(SchemaName, Key) -> db_edit:delete(SchemaName, Key, schema_specifications()).

clear_all_tables() -> db_edit:clear_all_tables(schema_specifications()).


%-------------------------------------------------------
%                     Utility Functions
%-------------------------------------------------------

build_schema_record_from_specifications(SchemaName) -> db_schemas:build_schema_record_from_specifications(SchemaName, schema_specifications()).

convert_schema_data_avp_list_into_record_tuple(AvpList) -> db_schemas:convert_schema_data_avp_list_into_record_tuple(AvpList, schema_specifications()).
