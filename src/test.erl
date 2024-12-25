-module(test).
-export([schema/0]).

schema() ->

    io:format("test::allocating a new schema map~n"),
    SS0 = mb_schemas:new(),
    io:format("test::is_map: ~p~n", [is_map(SS0)]),

    io:format("test::adding two new schema tables, employees and departments~n"),
    SS1 = mb_schemas:add_schema(employees, SS0),
    io:format("test::is_map: ~p~n", [is_map(SS1)]),

    SS2 = mb_schemas:add_schema(departments, SS1),
    io:format("test::is_map: ~p~n", [is_map(SS2)]),

    io:format("test::adding fields to employees~n"),
    SS3 = mb_schemas:add_field(employee_id, employees, SS2),
    io:format("test::is_map: ~p~n", [is_map(SS3)]),

    SS4 = mb_schemas:add_field(employee_last_name, employees, SS3),
    io:format("test::is_map: ~p~n", [is_map(SS4)]),

    SS5 = mb_schemas:add_field(employee_first_name, employees, SS4),
    io:format("test::is_map: ~p~n", [is_map(SS5)]),



    io:format("test::adding fields to departments~n"),
    SS6 = mb_schemas:add_field(department_id, departments, SS5),
    io:format("test::is_map: ~p~n", [is_map(SS6)]),

    SS7 = mb_schemas:add_field(manager_last_name, departments, SS6),
    io:format("test::is_map: ~p~n", [is_map(SS7)]),

    SS8 = mb_schemas:add_field(manager_first_name, departments, SS7),
    io:format("test::is_map: ~p~n", [is_map(SS8)]),

    io:format("test::setting field attributes to employee_id in employees~n"),
    SS9 = mb_schemas:set_field_attributes([{type, integer}, 
                                           {label, "Employee ID"}], 
                                           employee_id, 
                                           employees, SS8),
    io:format("test::is_map: ~p~n", [is_map(SS9)]),



    io:format("test::setting one field attribute <type> for employee_last_name in employees~n"),
    SS10 = mb_schemas:set_field_attribute(type, string, employee_last_name, employees, SS9),
    io:format("test::is_map: ~p~n", [is_map(SS10)]),


    io:format("test::setting field attributes to employee_first_name in employees~n"),
    SS11 = mb_schemas:set_field_attributes([{type, string}, 
                                         {label, "First Name"}, 
                                         {priority, optional}, 
                                         {default_value, ""}], 
                                          employee_first_name, 
                                          employees, SS10),
    io:format("test::is_map: ~p~n", [is_map(SS11)]),


    io:format("test::setting individual field attribute <type> for three fields in departments~n"),
    SS12 = mb_schemas:set_field_attribute(type, string, manager_last_name, departments, SS11),
    io:format("test::is_map: ~p~n", [is_map(SS12)]),

    SS13 = mb_schemas:set_field_attribute(type, string, manager_first_name, departments, SS12),
    io:format("test::is_map: ~p~n", [is_map(SS13)]),

    SS14 = mb_schemas:set_field_attribute(type, string, department_id, departments, SS13),
    io:format("test::is_map: ~p~n", [is_map(SS14)]),

    io:format("test::schema specifications: ~p~n", [SS14]),
    io:format("test::generating specifications~n"),
    mb_schemas:generate(test_schema, SS14).


    