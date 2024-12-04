-module(test_schema).

-export([schema_specifications/0]).
-export([install/0, install/1, start/0, stop/0, table_size/1, table_sizes/0]).
-export([schema_names/0, is_schema/1, is_field/2, schemas/0, get_schema/1, get_schema_attribute/2]).
-export([fields/1, field_count/1, field_names/1, key_name/1, key_type/1, field_position/2, get_field_attribute/3]).
-export([read/2, select/4, select_or/6, select_and/6, build_matchhead/1]).
-export([add/3, delete/2, clear_all_tables/0]).
-export([build_record_from_specifications/1, validate_record/1]).

schema_specifications() ->
    #{version => "0.2",
      schemas =>
          [{employees,
               #{name => employees,type => set,
                 fields =>
                     [{employee_id,
                          #{label => "Employee ID",name => employee_id,
                            position => 1,priority => mandatory,
                            type => integer,description => [],
                            default_value => not_defined,role => key}},
                      {employee_last_name,
                          #{label => [],name => employee_last_name,
                            position => 2,priority => mandatory,
                            type => string,description => [],
                            default_value => not_defined,role => field}},
                      {employee_first_name,
                          #{label => "First Name",name => employee_first_name,
                            position => 3,priority => optional,type => string,
                            description => [],default_value => [],
                            role => field}}],
                 disc_copies => [],disc_only_copies => [],ram_copies => []}},
           {departments,
               #{name => departments,type => set,
                 fields =>
                     [{department_id,
                          #{label => [],name => department_id,position => 1,
                            priority => mandatory,type => not_defined,
                            description => [],default_value => not_defined,
                            role => key}},
                      {manager_last_name,
                          #{label => [],name => manager_last_name,
                            position => 2,priority => mandatory,
                            type => string,description => [],
                            default_value => not_defined,role => field}},
                      {manager_first_name,
                          #{label => [],name => manager_first_name,
                            position => 3,priority => mandatory,
                            type => string,description => [],
                            default_value => not_defined,role => field}}],
                 disc_copies => [],disc_only_copies => [],ram_copies => []}}]}.


%-------------------------------------------------------
%                DB Management Functions
%-------------------------------------------------------

install() -> manage_db:install(schema_specifications()).

install(NodeList) -> manage_db:install(NodeList, schema_specifications()).

start() -> manage_db:start(schema_specifications()).

stop() -> manage_db:stop().

table_size(SchemaName) -> manage_db:table_size(SchemaName).

table_sizes() -> manage_db:table_sizes(schema_specifications()).


%-------------------------------------------------------
%                     Schema Functions
%-------------------------------------------------------

schema_names() -> schemas:schema_names(schema_specifications()).

is_schema(SchemaName) -> schemas:is_schema(SchemaName, schema_specifications()).

is_field(FieldName, SchemaName) -> schemas:is_field(FieldName, SchemaName, schema_specifications()).

schemas() -> schemas:schemas(schema_specifications()).

get_schema(SchemaName) -> schemas:get_schema(SchemaName, schema_specifications()).

get_schema_attribute(Attribute, SchemaName) -> schemas:get_schema_attribute(Attribute, SchemaName, schema_specifications()).

fields(SchemaName) -> schemas:fields(SchemaName, schema_specifications()).

field_count(SchemaName) -> schemas:field_count(SchemaName, schema_specifications()).

field_names(SchemaName) -> schemas:field_names(SchemaName, schema_specifications()).

key_name(SchemaName) -> schemas:key_name(SchemaName, schema_specifications()).

key_type(SchemaName) -> schemas:key_type(SchemaName, schema_specifications()).

field_position(FieldNAme, SchemaName) -> schemas:field_position(FieldNAme, SchemaName, schema_specifications()).

get_field_attribute(Attribute, FieldName, SchemaName) -> schemas:get_field_attribute(Attribute, FieldName, SchemaName, schema_specifications()).


%-------------------------------------------------------
%                     Query Functions
%-------------------------------------------------------

read(Table, Key) -> query_db:read(Table, Key).

select(Table, Field, Oper, Value) -> query_db:select(Table, Field, Oper, Value, schema_specifications()).

select_or(Table, Field, Oper1, Value1, Oper2, Value2) -> query_db:select_or(Table, Field, Oper1, Value1, Oper2, Value2, schema_specifications()).

select_and(Table, Field, Oper1, Value1, Oper2, Value2) -> query_db:select_and(Table, Field, Oper1, Value1, Oper2, Value2, schema_specifications()).

build_matchhead(Table) -> query_db:build_matchhead(Table, schema_specifications()).


%-------------------------------------------------------
%                     Modify Functions
%-------------------------------------------------------

add(Table, Key, Data) -> modify_db:add(Table, Key, Data).

delete(Table, Key) -> modify_db:delete(Table, Key).

clear_all_tables() -> modify_db:clear_all_tables().


%-------------------------------------------------------
%                     Utility Functions
%-------------------------------------------------------

build_record_from_specifications(SchemaName) -> schemas:build_record_from_specifications(SchemaName, schema_specifications()).

validate_record(Record) -> schemas:validate_record(Record, schema_specifications()).
