-module(mb_test).
-export([generate/0, c/0, delete_beam/0, test_api/0]).

-define(SSG_NAME, test_schema).


%-------------------------------------------------------------
%-------------------------------------------------------------
generate() ->

    %io:format("test::allocating a new schema map~n"),
    SS0 = mb_ssg:new(?SSG_NAME, "Test User", "nowhere@nowehre.com", "Test module to validate the schema handling code"),
    %io:format("test::is_map: ~p~n", [is_map(SS0)]),

    %io:format("test::adding two new schema tables, employees and departments~n"),
    SS1 = mb_ssg:add_schema(employees, SS0),
    %io:format("test::is_map: ~p~n", [is_map(SS1)]),

    SS2 = mb_ssg:add_schema(departments, SS1),
    %io:format("test::is_map: ~p~n", [is_map(SS2)]),

    %io:format("test::adding fields to employees~n"),
    SS3 = mb_ssg:add_field(employee_id, employees, SS2),
    %io:format("test::is_map: ~p~n", [is_map(SS3)]),

    SS4 = mb_ssg:add_field(employee_last_name, employees, SS3),
    %io:format("test::is_map: ~p~n", [is_map(SS4)]),

    SS5 = mb_ssg:add_field(employee_first_name, employees, SS4),
    %io:format("test::is_map: ~p~n", [is_map(SS5)]),



    %io:format("test::adding fields to departments~n"),
    SS6 = mb_ssg:add_field(department_id, departments, SS5),
    %io:format("test::is_map: ~p~n", [is_map(SS6)]),

    SS7 = mb_ssg:add_field(manager_last_name, departments, SS6),
    %io:format("test::is_map: ~p~n", [is_map(SS7)]),

    SS8 = mb_ssg:add_field(manager_first_name, departments, SS7),
    %io:format("test::is_map: ~p~n", [is_map(SS8)]),

    %io:format("test::setting field attributes to employee_id in employees~n"),
    SS9 = mb_ssg:set_field_attributes([{type, integer}, 
                                           {label, "Employee ID"}], 
                                           employee_id, 
                                           employees, SS8),
    %io:format("test::is_map: ~p~n", [is_map(SS9)]),



    %io:format("test::setting one field attribute <type> for employee_last_name in employees~n"),
    SS10 = mb_ssg:set_field_attribute(type, string, employee_last_name, employees, SS9),
    %io:format("test::is_map: ~p~n", [is_map(SS10)]),


    %io:format("test::setting field attributes to employee_first_name in employees~n"),
    SS11 = mb_ssg:set_field_attributes([{type, string}, 
                                         {label, "First Name"}, 
                                         {priority, optional}, 
                                         {default_value, ""}], 
                                          employee_first_name, 
                                          employees, SS10),
    %io:format("test::is_map: ~p~n", [is_map(SS11)]),


    %io:format("test::setting individual field attribute <type> for three fields in departments~n"),
    SS12 = mb_ssg:set_field_attribute(type, string, manager_last_name, departments, SS11),
    %io:format("test::is_map: ~p~n", [is_map(SS12)]),

    SS13 = mb_ssg:set_field_attribute(type, string, manager_first_name, departments, SS12),
    %io:format("test::is_map: ~p~n", [is_map(SS13)]),

    SS14 = mb_ssg:set_field_attribute(type, string, department_id, departments, SS13),
    %io:format("test::is_map: ~p~n", [is_map(SS14)]),

    %io:format("test::schema specifications: ~p~n", [SS14]),
    %io:format("test::generating specifications~n"),

    SS15 = mb_ssg:add_schema_disc_copies([node()], employees, SS14),
    SS16 = mb_ssg:add_schema_disc_copies([node()], departments, SS15),

    mb_ssg:generate(?SSG_NAME, SS16).

%-------------------------------------------------------------
%-------------------------------------------------------------
c() -> compile().

compile() ->
    %% Get all .erl files in ../src directory
    {ok, Files} = file:list_dir("../src"),
    ErlFiles = [Filename || Filename <- Files, filename:extension(Filename) =:= ".erl"],

    delete_beam(),

    %% Compile each .erl file
    lists:foreach(fun(Filename) ->
        compile_file(Filename)
    end, ErlFiles).

%-------------------------------------------------------------
%-------------------------------------------------------------
compile_file(Filename) ->
    %% Generate the full path to the file
    FilePath = filename:join("../src", Filename),
    
    case compile:file(FilePath, [{outdir, "./"}, report_errors]) of
        {ok, Module} -> 
            case code:soft_purge(Module) of 
                true ->
                    case code:load_file(Module) of 
                        {module, Module} -> Result = "ok";
                        _ -> Result = "load failed" 
                    end;
                false -> Result = "purge failed" 
            end;
        _ -> Result = "compile failed" 
    end,

    ModuleName = lists:sublist(Filename, length(Filename) - 4),
    io:format("~-30s~s~n", [ModuleName, Result]).


%-------------------------------------------------------------
%-------------------------------------------------------------
delete_beam() ->
    %% Get all .erl files in ../src directory
    {ok, Files} = file:list_dir("../ebin"),
    ErlFiles = [Filename || Filename <- Files, filename:extension(Filename) =:= ".beam"],

    %% Delete each .beam file
    lists:foreach(fun(Filename) -> file:delete("../ebin/" ++ Filename) end, ErlFiles).


%-------------------------------------------------------------
%-------------------------------------------------------------
test_api() -> 

    {ok, S} = mb_api:connect(),
    TableName = my_table,
    SsgName = test_ssg,

    Result = ok,

    Result = mb_api:set_ssg_name(S, SsgName),
    %Result = mb_api:new_ssg(S, SsgName, "All Things Erlang", "haitham@gmail.com", "SSG used by SsgName"),
    Result = mb_api:add_schema(S, TableName),
    Result = mb_api:set_schema_type(S, TableName, bag),

    Result = mb_api:add_schema_ram_copies_server(S, TableName),
    Result = mb_api:add_schema_disc_copies_cluster(S, TableName),
    %Result = mb_api:add_schema_disc_only_copies(S, TableName, [fakenode@nowhere]),

    %Result = mb_api:add_schema_ram_copies(S, TableName, ['n1@a.com', 'n2@a.com']),
    %Result = mb_api:add_schema_disc_copies(S, TableName, ['n3@a.com', 'n4@a.com']),
    %Result = mb_api:add_schema_disc_only_copies(S, TableName, ['n5@a.com', 'n6@a.com']),

    %Result = mb_api:delete_schema_ram_copies(S, TableName, ['n1@a.com']),
    %Result = mb_api:delete_schema_disc_copies(S, TableName, ['n3@a.com', 'n10@a.com']),
    %Result = mb_api:delete_schema_disc_only_copies(S, TableName, ['n5@a.com', 'n26@a.com']),

    %Result = mb_api:delete_schema_ram_copies_server(S, TableName),
    %Result = mb_api:delete_schema_disc_copies_server(S, TableName),
    %Result = mb_api:delete_schema_disc_only_copies_server(S, TableName),

    %Result = mb_api:delete_schema_ram_copies_cluster(S, TableName),
    %Result = mb_api:delete_schema_disc_copies_cluster(S, TableName),
    %Result = mb_api:delete_schema_disc_only_copies_cluster(S, TableName),



    io:format("SSG: ~n~p~n~n", [mb_api:get_ssg(S)]),
    io:format("generate result: ~p~n", [mb_api:generate(S)]),
    %io:format("  deploy result: ~p~n", [mb_api:deploy(S)]),

    mb_api:disconnect(S).




    