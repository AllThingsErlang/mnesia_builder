-module(ssg1).

-export([get_ssg/0]).
-export([install/0, install/1, start/0, stop/0, table_size/1, table_sizes/0]).
-export([schema_names/0, is_schema/1, is_field/2, schemas/0, get_schema/1, get_schema_attribute/2]).
-export([fields/1, field_count/1, mandatory_field_count/1, field_names/1, key_name/1, key_type/1, field_position/2, get_field_attribute/3]).
-export([read/2, select/4, select_or/6, select_and/6, build_matchhead/1]).
-export([add/1, add/2, add/3, delete/1, delete/2, clear_all_tables/0]).
-export([build_schema_record_from_specifications/1, convert_schema_data_avp_list_into_record_tuple/1]).

get_ssg() ->
    #{name => [],owner => [],version => "0.2",description => [],email => [],
      schemas =>
          [{employees,
               #{name => employees,type => set,description => [],
                 fields =>
                     [{id,#{label => [],name => id,position => 1,
                            priority => mandatory,type => term,
                            description => [],default_value => not_defined,
                            role => key}},
                      {last_name,
                          #{label => [],name => last_name,position => 2,
                            priority => mandatory,type => term,
                            description => [],default_value => not_defined,
                            role => field}},
                      {first_name,
                          #{label => [],name => first_name,position => 3,
                            priority => mandatory,type => term,
                            description => [],default_value => not_defined,
                            role => field}}],
                 disc_copies => [],disc_only_copies => [],ram_copies => []}}],
      created => {{2024,12,26},{13,49,50}}}.


%-------------------------------------------------------
%                DB Management Functions
%-------------------------------------------------------

install() -> mb_db_management:install(get_ssg()).

install(NodeList) -> mb_db_management:install(NodeList, get_ssg()).

start() -> mb_db_management:start(get_ssg()).

stop() -> mb_db_management:stop().

table_size(SchemaName) -> mb_db_management:table_size(SchemaName, get_ssg()).

table_sizes() -> mb_db_management:table_sizes(get_ssg()).


%-------------------------------------------------------
%                     Schema Functions
%-------------------------------------------------------

schema_names() -> mb_schemas:schema_names(get_ssg()).

is_schema(SchemaName) -> mb_schemas:is_schema(SchemaName, get_ssg()).

is_field(FieldName, SchemaName) -> mb_schemas:is_field(FieldName, SchemaName, get_ssg()).

schemas() -> mb_schemas:schemas(get_ssg()).

get_schema(SchemaName) -> mb_schemas:get_schema(SchemaName, get_ssg()).

get_schema_attribute(Attribute, SchemaName) -> mb_schemas:get_schema_attribute(Attribute, SchemaName, get_ssg()).

fields(SchemaName) -> mb_schemas:fields(SchemaName, get_ssg()).

field_count(SchemaName) -> mb_schemas:field_count(SchemaName, get_ssg()).

mandatory_field_count(SchemaName) -> mb_schemas:mandatory_field_count(SchemaName, get_ssg()).

field_names(SchemaName) -> mb_schemas:field_names(SchemaName, get_ssg()).

key_name(SchemaName) -> mb_schemas:key_name(SchemaName, get_ssg()).

key_type(SchemaName) -> mb_schemas:key_type(SchemaName, get_ssg()).

field_position(FieldName, SchemaName) -> mb_schemas:field_position(FieldName, SchemaName, get_ssg()).

get_field_attribute(Attribute, FieldName, SchemaName) -> mb_schemas:get_field_attribute(Attribute, FieldName, SchemaName, get_ssg()).


%-------------------------------------------------------
%                     Query Functions
%-------------------------------------------------------

read(SchemaName, Key) -> mb_db_query:read(SchemaName, Key).

select(SchemaName, Field, Oper, Value) -> mb_db_query:select(SchemaName, Field, Oper, Value, get_ssg()).

select_or(SchemaName, Field, Oper1, Value1, Oper2, Value2) -> mb_db_query:select_or(SchemaName, Field, Oper1, Value1, Oper2, Value2, get_ssg()).

select_and(SchemaName, Field, Oper1, Value1, Oper2, Value2) -> mb_db_query:select_and(SchemaName, Field, Oper1, Value1, Oper2, Value2, get_ssg()).

build_matchhead(SchemaName) -> mb_db_query:build_matchhead(SchemaName, get_ssg()).


%-------------------------------------------------------
%                     Modify Functions
%-------------------------------------------------------

add(Record) -> mb_db_edit:add(Record, get_ssg()).

add(SchemaName, Record) -> mb_db_edit:add(SchemaName, Record, get_ssg()).

add(SchemaName, Key, Data) -> mb_db_edit:add(SchemaName, Key, Data, get_ssg()).

delete(TableKey) -> mb_db_edit:delete(TableKey, get_ssg()).

delete(SchemaName, Key) -> mb_db_edit:delete(SchemaName, Key, get_ssg()).

clear_all_tables() -> mb_db_edit:clear_all_tables(get_ssg()).


%-------------------------------------------------------
%                     Utility Functions
%-------------------------------------------------------

build_schema_record_from_specifications(SchemaName) -> mb_schemas:build_schema_record_from_specifications(SchemaName, get_ssg()).

convert_schema_data_avp_list_into_record_tuple(AvpList) -> mb_schemas:convert_schema_data_avp_list_into_record_tuple(AvpList, get_ssg()).
