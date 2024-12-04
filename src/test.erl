-module(test).
-export([run/0]).

run() ->

    SS0 = schemas:new(),
    SS1 = schemas:add_schema(employees, SS0),
    SS2 = schemas:add_schema(departments, SS1),

    SS3 = schemas:add_field(employee_id, employees, SS2),
    SS4 = schemas:add_field(employee_last_name, employees, SS3),
    SS5 = schemas:add_field(employee_first_name, employees, SS4),

    SS6 = schemas:add_field(department_id, departments, SS5),
    SS7 = schemas:add_field(manager_last_name, departments, SS6),
    SS8 = schemas:add_field(manager_first_name, departments, SS7),

    SS9 = schemas:set_field_attributes([{type, integer}, 
                                        {label, "Employee ID"}], 
                                        employee_id, 
                                        employees, 
                                        SS8),

    SS10 = schemas:set_field_attribute(type, string, employee_last_name, employees, SS9),

    SS11 = schemas:set_field_attributes([{type, string}, 
                                         {label, "First Name"}, 
                                         {priority, optional}, 
                                         {default_value, ""}], 
                                          employee_first_name, 
                                          employees, SS10),

    SS12 = schemas:set_field_attribute(type, string, manager_last_name, departments, SS11),
    SS13 = schemas:set_field_attribute(type, string, manager_first_name, departments, SS12),
    SS14 = schemas:set_field_attribute(type, string, department_id, departments, SS13),

    schemas:generate(test_schema, SS14).


