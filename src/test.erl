-module(test).
-export([schema/0]).

schema() ->

    io:format("test::allocating a new schema map~n"),
    SS0 = db_schemas:new(),

    io:format("test::adding two new schema tables, employees and departments~n"),
    SS1 = db_schemas:add_schema(employees, SS0),
    SS2 = db_schemas:add_schema(departments, SS1),

    io:format("test::adding fields to employees~n"),
    SS3 = db_schemas:add_field(employee_id, employees, SS2),
    SS4 = db_schemas:add_field(employee_last_name, employees, SS3),
    SS5 = db_schemas:add_field(employee_first_name, employees, SS4),

    io:format("test::adding fields to departments~n"),
    SS6 = db_schemas:add_field(department_id, departments, SS5),
    SS7 = db_schemas:add_field(manager_last_name, departments, SS6),
    SS8 = db_schemas:add_field(manager_first_name, departments, SS7),

    io:format("test::setting field attributes to employee_id in employees~n"),
    SS9 = db_schemas:set_field_attributes([{type, integer}, 
                                           {label, "Employee ID"}], 
                                           employee_id, 
                                           employees, SS8),

    io:format("test::setting one field attribute <type> for employee_last_name in employees~n"),
    SS10 = db_schemas:set_field_attribute(type, string, employee_last_name, employees, SS9),

    io:format("test::setting field attributes to employee_first_name in employees~n"),
    SS11 = db_schemas:set_field_attributes([{type, string}, 
                                         {label, "First Name"}, 
                                         {priority, optional}, 
                                         {default_value, ""}], 
                                          employee_first_name, 
                                          employees, SS10),

    io:format("test::setting individual field attribute <type> for three fields in departments~n"),
    SS12 = db_schemas:set_field_attribute(type, string, manager_last_name, departments, SS11),
    SS13 = db_schemas:set_field_attribute(type, string, manager_first_name, departments, SS12),
    SS14 = db_schemas:set_field_attribute(type, string, department_id, departments, SS13),

    io:format("test::generating specifications~n"),
    db_schemas:generate(test_schema, SS14).


    