-module(mb_cli_menu).
-export([start/1]).

-define(N, 10).
-define(CMD_INSTALL, "install").
-define(CMD_START, "start").
-define(CMD_STOP, "stop").
-define(CMD_TABLES, "tables").
-define(CMD_SIZE, "size").
-define(CMD_ADD, "add").
-define(CMD_DEL, "del").
-define(CMD_LOAD, "load").
-define(CMD_WIPE, "wipe").
-define(CMD_FIELDS, "fields").
-define(CMD_OPER, "oper").
-define(CMD_READ, "read").
-define(CMD_QUERY, "q").
-define(CMD_QOR, "qor").
-define(CMD_QAND, "qand").
-define(CMD_QDL, "qdl").
-define(CMD_CLS, "cls").
-define(CMD_HELP, "help").
-define(CMD_EXIT, "exit").

%-------------------------------------------------------------
% Function: menu
% Purpose:  Displays the user menu
% Returns:  
%-------------------------------------------------------------
start(SchemaModule) -> 
    QueryModules = load_query_modules(),
    %%clear_screen(),
    process_help(),
    prompt({SchemaModule, QueryModules}).


%-------------------------------------------------------------
% Function: menu
% Purpose:  Displays the user menu
% Returns:  
%-------------------------------------------------------------
prompt(LoadedModules) -> 
    io:format("~nmpt> "),
    Input = get_user_input(),
    process_user_input(Input, LoadedModules).

%-------------------------------------------------------------
% Function: menu
% Purpose:  Displays the user menu
% Returns:  
%-------------------------------------------------------------
process_help() ->
    
    io:format("~n"),
    io:format("-------------------------------------~n"),
    io:format("admin commands:~n"),
    io:format("   - ~-10s ... ~s~n", [?CMD_INSTALL, command_info(?CMD_INSTALL, description)]),
    io:format("   - ~-10s ... ~s~n", [?CMD_START, command_info(?CMD_START, description)]),
    io:format("   - ~-10s ... ~s~n", [?CMD_STOP, command_info(?CMD_STOP, description)]),
    io:format("   - ~-10s ... ~s~n", [?CMD_TABLES, command_info(?CMD_TABLES, description)]),
    io:format("   - ~-10s ... ~s~n", [?CMD_SIZE, command_info(?CMD_SIZE, description)]),
    io:format("~n"),
    io:format("data modification commands:~n"),
    io:format("   - ~-10s ... ~s~n", [?CMD_ADD, command_info(?CMD_ADD, description)]),
    io:format("   - ~-10s ... ~s~n", [?CMD_DEL, command_info(?CMD_DEL, description)]),
    io:format("   - ~-10s ... ~s~n", [?CMD_LOAD, command_info(?CMD_LOAD, description)]),
    io:format("   - ~-10s ... ~s~n", [?CMD_WIPE, command_info(?CMD_WIPE, description)]),
    io:format("~n"),
    io:format("query commands:~n"),
    io:format("   - ~-10s ... ~s~n", [?CMD_FIELDS, command_info(?CMD_FIELDS, description)]),
    io:format("   - ~-10s ... ~s~n", [?CMD_OPER, command_info(?CMD_OPER, description)]),
    io:format("   - ~-10s ... ~s~n", [?CMD_READ, command_info(?CMD_READ, description)]),
    io:format("   - ~-10s ... ~s~n", [?CMD_QUERY, command_info(?CMD_QUERY, description)]),
    io:format("   - ~-10s ... ~s~n", [?CMD_QOR, command_info(?CMD_QOR, description)]),
    io:format("   - ~-10s ... ~s~n", [?CMD_QAND, command_info(?CMD_QAND, description)]),
    io:format("   - ~-10s ... ~s~n", [?CMD_QDL, command_info(?CMD_QDL, description)]),
    io:format("~n"),
    io:format("-------------------------------------~n"),
    io:format("   - ~-10s ... ~s~n", [?CMD_CLS, command_info(?CMD_CLS, description)]),
    io:format("   - ~-10s ... ~s~n", [?CMD_HELP, command_info(?CMD_HELP, description)]),
    io:format("   - ~-10s ... ~s~n", [?CMD_EXIT, command_info(?CMD_EXIT, description)]),
    io:format("-------------------------------------~n"),
    io:format("~n").


%-------------------------------------------------------------
% Function: menu
% Purpose:  Displays the user menu
% Returns:  
%-------------------------------------------------------------
process_help(Arg1) -> io:format("~n~s~n", [command_info(Arg1, syntax)]).

%-------------------------------------------------------------
% Function: menu
% Purpose:  Displays the user menu
% Returns:  
%-------------------------------------------------------------
command_info(Command, Info) ->

    case Command of

        ?CMD_INSTALL ->
            case Info of
                description -> "install schemas";
                syntax -> ?CMD_INSTALL
            end;

        ?CMD_START   ->
            case Info of
                description -> "start mnesia application";
                syntax -> ?CMD_START
            end;

        ?CMD_STOP    ->
            case Info of
                description -> "stop mnesia application";
                syntax -> ?CMD_STOP
            end;

        ?CMD_TABLES  ->
            case Info of
                description -> "display table names";
                syntax -> ?CMD_TABLES ++ " [table_name]"
            end;

        ?CMD_SIZE    ->
            case Info of
                description -> "display table sizes";
                syntax -> ?CMD_SIZE ++ " [table_name]"
            end;

        ?CMD_ADD     -> 
            case Info of
                description -> "add record to table";
                syntax -> ?CMD_ADD ++ " <table_name> <key_value> <field_value> [field_value]*"
            end;

        ?CMD_DEL     -> 
            case Info of
                description -> "delete record from table";
                syntax -> ?CMD_DEL ++ " <table_name> <key_value>"
            end;

        ?CMD_LOAD    -> 
            case Info of
                description -> "load data into table from csv file";
                syntax -> ?CMD_LOAD ++ " <table_name> <file_path/file_name.csv> (not supported)"
            end;

        ?CMD_WIPE    -> 
            case Info of
                description -> "wipe data from table or all tables";
                syntax -> ?CMD_WIPE ++ " [table_name]"
            end;

        ?CMD_FIELDS  -> 
            case Info of
                description -> "display table field names, 1st field is key";
                syntax -> ?CMD_FIELDS ++ " [table_name]"
            end;

        ?CMD_OPER    -> 
            case Info of
                description -> "display supported operators";
                syntax -> ?CMD_OPER
            end;

        ?CMD_READ    -> 
            case Info of
                description -> "read from table";
                syntax -> ?CMD_READ ++ " <table_name> <key_value>"
            end;

        ?CMD_QUERY   -> 
            case Info of
                description -> "query table based on one field and one operator comparison";
                syntax -> ?CMD_QUERY ++ " <table_name> <field_name> <oper> <value>"
            end;

        ?CMD_QOR     -> 
            case Info of
                description -> "query table based on one field using OR";
                syntax -> ?CMD_QOR ++ " <table_name> <field_name> <oper1> <value1> <oper2> <value2>"
            end;

        ?CMD_QAND    -> 
            case Info of
                description -> "query table based on one field using AND";
                syntax -> ?CMD_QAND ++ " <table_name> <field_name> <oper1> <value1> <oper2> <value2>"
            end;

        ?CMD_QDL     -> 
            case Info of
                description -> "run prebuilt query from dynamically loaded modules";
                syntax -> ?CMD_QDL ++ " (follow prompts)"
            end;

        ?CMD_CLS -> 
            case Info of 
                description -> "clear the screen";
                syntax -> ?CMD_CLS
            end;

        ?CMD_HELP    -> 
            case Info of
                description -> "run the help command";
                syntax -> ?CMD_HELP ++ " [command_name]"
            end;

        ?CMD_EXIT    -> 
            case Info of
                description -> "exit the menu and return to Erlang shell";
                syntax -> ?CMD_EXIT
            end
    end.


%-------------------------------------------------------------
% Function: process_admin_menu_input
% Purpose:  
% Returns:  
%-------------------------------------------------------------
process_user_input(Input, LoadedModules) ->

    {SchemaModule, _} = LoadedModules,

    case string:prefix(Input, ?CMD_ADD) of 
        nomatch -> SpecialParse = false;
        Remainder -> 
            SpecialParse = true,
            process_add(Remainder ++ ".", LoadedModules)
    end,

    case SpecialParse of 
        true -> prompt(LoadedModules); 
            
        false ->
            Tokens = string:tokens(Input, " " ),

            case Tokens of

                [Command] ->

                    case Command of
                        ?CMD_INSTALL ->
                            SchemaModule:install(),
                            prompt(LoadedModules);
                        
                        ?CMD_TABLES ->
                            process_tables(),
                            prompt(LoadedModules);
                            
                        ?CMD_START -> 
                            process_start(LoadedModules),
                            prompt(LoadedModules);

                        ?CMD_STOP -> 
                            process_stop(LoadedModules),
                            prompt(LoadedModules);

                        ?CMD_SIZE -> 
                            process_size([], LoadedModules),
                            prompt(LoadedModules);

                        ?CMD_WIPE ->
                            process_wipe([], LoadedModules),
                            prompt(LoadedModules);

                        ?CMD_FIELDS ->
                            process_fields([], LoadedModules),
                            prompt(LoadedModules);

                        ?CMD_OPER ->
                            process_oper(),
                            prompt(LoadedModules);

                        ?CMD_QDL ->
                            process_qdl(LoadedModules),
                            prompt(LoadedModules);

                        ?CMD_CLS ->
                            clear_screen(),
                            prompt(LoadedModules);

                        ?CMD_HELP -> 
                            process_help(),
                            prompt(LoadedModules);

                        ?CMD_EXIT -> ok;

                        "" -> prompt(LoadedModules);
                        _ -> 
                            invalid_input(),
                            prompt(LoadedModules)
                    end;

                [Command, Arg1] ->

                    case Command of

                        ?CMD_SIZE -> 
                            process_size(list_to_atom(Arg1), LoadedModules),
                            prompt(LoadedModules);

                        ?CMD_WIPE ->
                            process_wipe(list_to_atom(Arg1), LoadedModules),
                            prompt(LoadedModules);

                        ?CMD_FIELDS ->
                            process_fields(list_to_atom(Arg1), LoadedModules),
                            prompt(LoadedModules);

                        ?CMD_HELP ->
                            process_help(Arg1),
                            prompt(LoadedModules);

                        _ -> 
                            invalid_input(),
                            prompt(LoadedModules)
                    end;

                [Command, Arg1, Arg2] -> 

                    case Command of

                        ?CMD_DEL -> 

                            process_del(Arg1, Arg2, LoadedModules),
                            prompt(LoadedModules);

                        ?CMD_READ -> 

                            process_read(Arg1, Arg2, LoadedModules),
                            prompt(LoadedModules);

                        _ ->
                            invalid_input(),
                            prompt(LoadedModules)
                    end;

                [Command, Arg1, Arg2, Arg3, Arg4] ->

                    case Command of 
                        
                        ?CMD_QUERY -> 
                            process_q(Arg1, Arg2, Arg3, Arg4, LoadedModules),
                            prompt(LoadedModules);

                        _ -> 
                            invalid_input(),
                            prompt(LoadedModules)

                    end;

                [Command, Arg1, Arg2, Arg3, Arg4, Arg5, Arg6] ->

                    case Command of 
                        
                        ?CMD_QOR ->
                            process_qor(Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, LoadedModules),
                            prompt(LoadedModules);

                        ?CMD_QAND ->
                            process_qand(Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, LoadedModules),
                            prompt(LoadedModules);

                        _ -> 
                            invalid_input(),
                            prompt(LoadedModules)

                    end;
                
                _ -> 
                    invalid_input(),
                    prompt(LoadedModules)
            end
    end.


        %"load" -> 
        %    io:format("File: "),
        %    File = get_user_input(),
        %    case load_from_csv(File) of
        %        ok -> io:format("Data loaded~n~n");
        %        {error, Reason} -> io:format("~s~n~n", [Reason])
        %    end,



%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
invalid_input() -> io:format("invalid input~n").


%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
unknown_table(Table) -> io:format("table ~w not recognized ~n", [Table]).

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
mnesia_not_running() -> io:format("must start mnesia~n").


%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
process_start(LoadedModules) ->

    {SchemaModule, _} = LoadedModules,

    Result = SchemaModule:start(),
    io:format("result: ~w~n", [Result]).


%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
process_stop(LoadedModules) ->

    {SchemaModule, _} = LoadedModules,

    Result = SchemaModule:stop(),
    io:format("result: ~w~n", [Result]).


%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
process_oper() ->
    io:format("   ==~n"),
    io:format("   =:=~n"),
    io:format("   >=~n"),
    io:format("   =<~n"),
    io:format("   /=~n").

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
process_fields(Table, LoadedModules) -> 

    {SchemaModule, _} = LoadedModules,

    % This function makes a decision, is it all tables or just one.
    % And then calls the proper processor.
    case Table of
        [] -> display_fields_all_tables(SchemaModule);
        _ -> display_fields_one_table(Table, SchemaModule)
    end.


%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
display_fields_one_table(Table, SchemaModule) -> 

    case SchemaModule:is_schema(Table) of
        true -> 
            io:format("~ntable: ~p~n~n", [Table]),
            display_field_info(Table, SchemaModule);
        false -> io:format("~p not a valid table in ~p. ~n", [Table, SchemaModule]) 
    end.


%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
display_field_info(Table, SchemaModule) -> 
    Fields = SchemaModule:field_names(Table),
    display_field_info(Fields, Table, SchemaModule).

display_field_info([], _, _) -> io:format("~n");
display_field_info([Field | T], Table, SchemaModule) ->
    io:format("   ~-30w:  ~-10w  ~-12w~n", [Field, 
                                  SchemaModule:get_field_attribute(type, Field, Table),
                                  SchemaModule:get_field_attribute(priority, Field, Table)]),

    display_field_info(T, Table, SchemaModule).

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
display_fields_all_tables(SchemaModule) ->
    display_fields_all_tables(SchemaModule:schema_names(), SchemaModule).

display_fields_all_tables([], _) -> io:format("~n");
display_fields_all_tables([H|T], SchemaModule) ->
    display_fields_one_table(H, SchemaModule),
    display_fields_all_tables(T, SchemaModule).

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
process_tables() ->

    case mnesia:system_info(running_db_nodes) of
        [] -> 
            mnesia_not_running();

        _ ->
            Tables = mnesia:system_info(tables),
            io:format("~n"),
            lists:foreach(fun(Table) -> io:format("   ~p~n", [Table]) end, Tables)
    end.

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
process_add(ArgumentsString, LoadedModules) ->

    {SchemaModule, _} = LoadedModules,

    case mb_utilities:parse_input_erlang_terms(ArgumentsString) of
        {ok, ParsedInput} -> 

            case convert_input_into_record_tuple(ParsedInput, SchemaModule) of 
                {error, Reason} -> io:format("error: ~p~n", [Reason]);
                
                RecordTuple -> 
                    case SchemaModule:add(RecordTuple) of 
                        ok -> io:format("added~n");
                        {error, Reason} -> io:format("error: ~p~n", [Reason])
                    end
            end;

        {error, _} -> io:format("~ninvalid input format~n")
    end.


%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
process_wipe(Table, LoadedModules) ->

    {SchemaModule, _} = LoadedModules,

    case mnesia:system_info(running_db_nodes) of
        [] -> mnesia_not_running();
        _ ->

        case Table of
            [] -> 
                io:format("clear *all* tables? [y/n]: "),
                Input = get_user_input(),

                case string:to_lower(Input) of
                    "y" -> 
                        {Cleared, NotCleared} = SchemaModule:clear_all_tables(),
                        io:format("cleared ~w out of ~w~n", [Cleared, Cleared+NotCleared]);

                    _ -> 
                        io:format("aborted~n")
                end;
            
            _ ->

                case lists:member(Table, SchemaModule:schema_names()) of
                    true ->
                        io:format("clear table ~w? [y/n]: ", [Table]),
                        Input = get_user_input(),

                        case string:to_lower(Input) of
                            "y" -> 
                                case mnesia:clear_table(Table) of
                                    {atomic, _} -> io:format("table ~w cleared~n", [Table]);
                                    {aborted, Reason} -> io:format("failed to clear table, ~w~n", [Reason])
                                end;

                            _ -> 
                                io:format("aborted~n")
                        end;

                    false ->
                        unknown_table(Table)
                end
        end
    end.

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
process_del(Arg1, Arg2, LoadedModules) ->

    {SchemaModule, _} = LoadedModules,

    case mnesia:system_info(running_db_nodes) of
        [] -> mnesia_not_running();

        _ ->

            % 1. Get the user provided table name and confirm it is valid
            % 2. Get the user provided key value and convert it 
            %    based on the expected type and confirm it is valid. 
            % 3. Execute the command

            Table = list_to_atom(Arg1),

            case lists:member(Table, SchemaModule:schema_names()) of
                true -> 

                    % Table name is valid
                    case mb_schemas:convert_from_stirng(Arg2, SchemaModule:key_type(Table)) of
                        
                        {error, Reason} -> io:format("delete failed, ~w~n", [Reason]);
                        {_, Key} ->
                            % Supplied key value is valid
                            Result = mb_db_edit:delete(Table, Key),
                    
                            case Result of
                                ok -> io:format("ok~n");
                                {_, Other} -> io:format("~w~n", [Other])
                            end
                    end;
                false -> 
                    unknown_table(Table)
            end
        end.


%-------------------------------------------------------------
% Function: 
%   read  Arg1          Arg2   
%   read  <table_name>  <key_value>
%
% Purpose:  
% Returns: 
%-------------------------------------------------------------
process_read(Arg1, Arg2, LoadedModules) ->

    {SchemaModule, _} = LoadedModules,

    case mnesia:system_info(running_db_nodes) of
        [] -> mnesia_not_running();

        _ ->
            
            Table = list_to_atom(Arg1),

            case lists:member(Table, SchemaModule:schema_names()) of
                true -> 
                    case mb_schemas:convert_from_stirng(Arg2, SchemaModule:key_type(Table)) of
                        
                        {error, Reason} -> io:format("read failed, ~w~n", [Reason]);
                        {_, Key} ->
                            Result = SchemaModule:read(Table, Key),
                            case Result of
                                {ok, Record} -> print_record(Record);
                                {_, Other} -> io:format("~w~n", [Other])
                            end
                    end;
                false -> unknown_table(Table)
            end
    end.


%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
process_q(Arg1, Arg2, Arg3, Arg4, LoadedModules) ->

    {SchemaModule, _} = LoadedModules,

    case mnesia:system_info(running_db_nodes) of
        [] -> mnesia_not_running();

        _ ->
            
            Table = list_to_atom(Arg1),
            Field = list_to_atom(Arg2),
            Oper = list_to_atom(Arg3),
            
            case lists:member(Table, SchemaModule:schema_names()) of
                true -> 
                    FieldType = SchemaModule:get_field_attribute(type, Field, Table),

                    case SchemaModule:is_field(Field, Table) of
                        true ->
                            case mb_utilities:is_comparison(Oper) of 
                                true ->
                                    case mb_schemas:convert_from_stirng(Arg4, FieldType) of
                                        {error, _Reaosn} -> io:format("invalid data type~n");
                                        {_, Value} -> 
                                            QueryOutput = SchemaModule:select(Table, Field, Oper, Value), 
                                            process_query_output(QueryOutput)
                                    end;
                                false -> io:format("not a comparison operator~n")
                            end;
                        false -> io:format("not a field in table~n")
                    end;
                false -> unknown_table(Table)
            end
    end.

%-------------------------------------------------------------
% Function: 
%      qor Arg1         Arg2         Arg3    Arg4     Arg5    Arg6
%      qor <table_name> <field_name> <oper1> <value1> <oper2> <value2>
%
% Purpose:  
% Returns: 
%-------------------------------------------------------------
process_qor(Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, LoadedModules) ->

    {SchemaModule, _} = LoadedModules,

    case mnesia:system_info(running_db_nodes) of
        [] -> mnesia_not_running();

        _ ->
            
            Table = list_to_atom(Arg1),
            Field = list_to_atom(Arg2),
            Oper1 = list_to_atom(Arg3),
            Oper2 = list_to_atom(Arg5),

            case lists:member(Table, SchemaModule:schema_names()) of
                true -> 
                    case SchemaModule:is_field(Field, Table) of
                        true ->
                            case (mb_utilities:is_comparison(Oper1) and mb_utilities:is_comparison(Oper2)) of 
                                true ->
                                    FieldType = SchemaModule:get_field_attribute(type, Field, Table),

                                    case mb_schemas:convert_from_stirng(Arg4,  FieldType) of
                                        {error, _Reaosn} -> io:format("invalid data type~n");
                                        {_, Value1} -> 
                                            case mb_schemas:convert_from_stirng(Arg6, FieldType) of
                                                {error, _Reason} -> io:format("invalid data type~n");
                                                {_, Value2} -> 
                                                    QueryOutput = SchemaModule:select_or(Table, Field, Oper1, Value1, Oper2, Value2), 
                                                    process_query_output(QueryOutput)
                                            end
                                    end;
                                false -> io:format("not a comparison operator~n")
                            end;
                        false -> io:format("not a field in table~n")
                    end;
                false -> unknown_table(Table)
            end
    end.

%-------------------------------------------------------------
% Function: 
%      qand Arg1         Arg2         Arg3    Arg4     Arg5    Arg6
%      qand <table_name> <field_name> <oper1> <value1> <oper2> <value2>
%
% Purpose:  
% Returns: 
%-------------------------------------------------------------
process_qand(Arg1, Arg2, Arg3, Arg4, Arg5, Arg6, LoadedModules) ->

    {SchemaModule, _} = LoadedModules,
    
    case mnesia:system_info(running_db_nodes) of
        [] -> mnesia_not_running();

        _ ->
            
            Table = list_to_atom(Arg1),
            Field = list_to_atom(Arg2),
            Oper1 = list_to_atom(Arg3),
            Oper2 = list_to_atom(Arg5),

            case lists:member(Table, SchemaModule:schema_names()) of
                true -> 
                    case SchemaModule:is_field(Field, Table) of
                        true ->
                            case (mb_utilities:is_comparison(Oper1) and mb_utilities:is_comparison(Oper2)) of 
                                true ->
                                    FieldType = SchemaModule:get_field_attribute(type, Field, Table),

                                    case mb_schemas:convert_from_stirng(Arg4, FieldType) of
                                        {error, _Reason} -> io:format("invalid data type~n");
                                        {_, Value1} -> 

                                            case mb_schemas:convert_from_stirng(Arg6, FieldType) of
                                                {error, _Reason} -> io:format("invalid data type~n");
                                                {_, Value2} -> 
                                                    QueryOutput = SchemaModule:select_and(Table, Field, Oper1, Value1, Oper2, Value2), 
                                                    process_query_output(QueryOutput)
                                            end;
                                        _ -> io:format("invalid data type~n")
                                    end;
                                false -> io:format("not a comparison operator~n")
                            end;
                        false -> io:format("not a field in table~n")
                    end;
                false -> unknown_table(Table)
            end
    end.

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
process_qdl(LoadedModules) ->

    {_, QueryModules} = LoadedModules,

    QueryModuleCount = length(QueryModules),

    case QueryModuleCount of

        0 -> 
            io:format("no dynamically loaded queries found~n");
        
        _ ->

            display_query_modules(QueryModules),
            io:format("~nselect query number or 0 to return: "),
            QueryNumberString = get_user_input(),

            case mb_schemas:convert_from_stirng(QueryNumberString, integer) of
                {error, _Reason} -> invalid_input();
                {integer, QueryNumber} ->
                    if 
                        QueryNumber == 0 ->
                            ok;

                        QueryNumber > QueryModuleCount; QueryNumber < 0 ->
                            io:format("invalid query number~n");

                        true ->
                            SelectedModule = lists:nth(QueryNumber, QueryModules),
                            QueryOutput = SelectedModule:select(),
                            process_query_output(QueryOutput)
                    end;
                _ -> {error, invalid_input}
            end
    end.
%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
write_query_result_to_file(Result) ->

    case Result of
        {ok, Records} -> write_my_record_list_of_records_to_file(Records);
        Records -> write_my_record_list_of_records_to_file(Records)
    end.


%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns:  
%-------------------------------------------------------------
write_my_record_list_of_records_to_file(Records) ->

    case create_csv_file() of
        {ok, File} ->
            write_my_record_list_of_records_to_file(File, Records);
        {error, Reason} -> {error, Reason}
    end.


write_my_record_list_of_records_to_file(File, []) -> 
    file:close(File),
    ok;
write_my_record_list_of_records_to_file(File, [Next|Remaining]) ->
        
    Record = convert_my_record_tuple_record_to_list_record(Next),

    write_csv_record(File, Record),
    write_my_record_list_of_records_to_file(File, Remaining).


%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns:  
%-------------------------------------------------------------
create_csv_file() ->
    case mb_utilities:create_timestamped_file("../reports") of
        {ok, File} -> 
            %write_csv_header(File, record_info(fields, schemas)),
            {ok, File};

        {error, Reason} -> {error, Reason}
    end.


%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns:  
%-------------------------------------------------------------
write_csv_header(File, []) -> file:write(File, "\n");
write_csv_header(File, [Field | Remaining]) -> 
    file:write(File, atom_to_list(Field)),
    case Remaining of
        [] -> write_csv_header(File, Remaining);
        _ -> 
            file:write(File, ","),
            write_csv_header(File, Remaining)
    end.

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns:  
%-------------------------------------------------------------
write_csv_record(File, []) -> file:write(File, "\n");
write_csv_record(File, [schemas | Remaining]) -> write_csv_record(File, Remaining);
write_csv_record(File, [Next | Remaining]) ->

    case is_float(Next) of
        true -> 
            FloatToString = io_lib:format("~p", [Next]),
            file:write(File, FloatToString);

        false -> file:write(File, Next)
    end,

    case Remaining of
        [] -> write_csv_record(File, Remaining);
        _ -> 
            file:write(File, ","),
            write_csv_record(File, Remaining)
    end.

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
process_query_output(QueryOutput) ->

    case QueryOutput of
        {ok, _} -> 

            io:format("~n~p~n~n", [QueryOutput]),

            case write_query_result_to_file(QueryOutput) of
                ok -> io:format("output written to file~n~n");
                {_, Other} -> io:format("~w~n~n", [Other])
            end;

        {error, not_found} -> io:format("not found~n");
        {error, {badarg, _}} -> invalid_input();
        {error, Other} -> io:format("failed: ~w~n", [Other])
    end.


%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
load_from_csv(FilePath) ->

    case file:open(FilePath, [read]) of
        {ok, File} ->
            % Skip the first line (header)
            io:get_line(File, ""),
            
            % Process each line individually
            process_lines(File),
            file:close(File),
            ok;
        {error, Reason} ->
            io:format("failed to open file, ~w~n", [Reason]),
            {error, Reason}
    end.

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
process_lines(File) ->
    case io:get_line(File, "") of
        eof -> ok;
        Line ->
            %io:format("~nloaded line: ~s", [Line]),
            % Parse the line and insert it into Mnesia
            parse_and_add(Line),
            % Continue processing the next line
            process_lines(File)
    end.

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
parse_and_add(Line) ->

    Fields = string:split(string:trim(Line), ",", all),

    queries:add(
        lists:nth(1, Fields),
        lists:nth(2, Fields),
        lists:nth(3, Fields),
        lists:nth(4, Fields),
        mb_utilities:string_to_float(lists:nth(5, Fields)),
        mb_utilities:string_to_float(lists:nth(6, Fields)),
        mb_utilities:string_to_float(lists:nth(7, Fields)),
        mb_utilities:string_to_float(lists:nth(8, Fields)),
        mb_utilities:string_to_float(lists:nth(9, Fields)),
        mb_utilities:string_to_float(lists:nth(10, Fields)),
        lists:nth(11, Fields),
        lists:nth(12, Fields),
        lists:nth(13, Fields)
        ).



%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns:  
%-------------------------------------------------------------
convert_my_record_tuple_record_to_list_record(Record) ->
    L1 = tuple_to_list(Record),
    case L1 of
        {schemas, L2} -> L2;
        _ -> L1
    end.


%-------------------------------------------------------------
% Function: get_user_input
% Purpose:  Reads user input from shell command line
% Returns:  
%-------------------------------------------------------------
get_user_input() -> string:trim(io:get_line("")).


%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
clear_screen() -> io:format("~c[2J~c[H", [27, 27]).



%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
mnesia_running_banner_warning() ->

    case mnesia:system_info(running_db_nodes) of
        [] -> 
            io:format("must start mnesia before using this command~n"),
            {error, not_running};
        _ -> ok
    end.


%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
process_size(Table, LoadedModules) ->

    {SchemaModule, _} = LoadedModules,

    case mnesia:system_info(running_db_nodes) of
        [] -> mnesia_not_running();
        _ ->

        case Table of
            [] -> display_list_of_tuples_of_pairs(SchemaModule:table_sizes());
            _ ->

                case lists:member(Table, SchemaModule:schema_names()) of
                    true ->
                        Result = SchemaModule:table_size(Table), 
                        io:format("~w ~w~n", [Table, Result]);
                    false ->
                        unknown_table(Table)
                end
        end
    end.


    
%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
load_query_modules() ->
    % Find all .beam files that start with "query" in the current directory
    Files = filelib:wildcard("userquery*.beam"),
    io:format("~w~n", [Files]),
    % Convert filenames to module names and load each one
    %lists:foreach(fun load_module/1, Files)
    load_modules(Files).

load_modules([]) -> [];
load_modules(Files) -> load_modules(Files, []).
load_modules([], Loaded) -> Loaded;
load_modules([Next|Remaining], Loaded) ->
    % Extract the module name from the file name (without ".beam" extension)
    ModuleName = list_to_atom(filename:basename(Next, ".beam")),
    
    % Load the module
    code:purge(ModuleName),

    case code:load_file(ModuleName) of
        {module, ModuleName} ->
            load_modules(Remaining, [ModuleName | Loaded]);
        {error, _} ->
            io:format("failed to load module: ~w~n", [ModuleName]),
            load_modules(Remaining, Loaded)
    end.

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
print_record(_Record) -> ok.

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
display_query_modules([]) -> ok;
display_query_modules(LoadedModules) -> display_query_modules(lists:reverse(LoadedModules), 1).

display_query_modules([], _) -> ok;
display_query_modules([Next | Remaining], N) ->

    io:format("   ~3w ~15w  ~s~n", [N, Next, Next:description()]),
    display_query_modules(Remaining, N+1).


%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
display_list_of_tuples_of_pairs([]) -> io:format("~n");
display_list_of_tuples_of_pairs([{A, B} | T]) -> 
    io:format("~w ~w~n", [A, B]),
    display_list_of_tuples_of_pairs(T).



%-------------------------------------------------------------
% Function: convert_input_into_record_tuple
%
% Purpose:  Converts the user input into an interim AVP list 
%           of the format:
%
%           [{schema, SchemaName}, {FieldName, FieldValue}, {FieldName, FieldValue}, ...]
%
%           This format is used for validating that we have all
%           the right fields with the right value types. Then
%           it is converted to the final format:
%
%           {SchemaName, FieldValue1, FieldValue2, ...}
%  
% Returns: 
%-------------------------------------------------------------
convert_input_into_record_tuple(Input, SchemaModule) when is_list(Input) -> 

    case length(Input) < 3 of 
        true -> {error, {invalid_field_count, Input}};
        false -> 

            [SchemaName | RemainingInput] = Input,

            % The input past the schema name must be in
            % form of <FieldName> <FieldValue> which means
            % a set of pairs (even count).
            case length(RemainingInput) rem 2 =:= 0 of 
                % If we have the right count, we build an AVP list
                % including the schema name. This list will
                % be used to validate the data and build the record tuple
                % in the form {SchemaName, FieldValue1, FieldValue2, ...}.
                true -> 
                    AvpList = convert_input_into_schema_data_avp_list(RemainingInput, [{schema, SchemaName}]),
                    SchemaModule:convert_schema_data_avp_list_into_record_tuple(AvpList);

                false -> {error, {invalid_field_count, Input}}
            end 
    end.

%-------------------------------------------------------------
% Function: 
% Purpose:
% Returns: 
%-------------------------------------------------------------
convert_input_into_schema_data_avp_list([], FinalAvpList) -> FinalAvpList;

convert_input_into_schema_data_avp_list(Input, InterimAvpList) ->

    [FieldName | RemainingInput1] = Input,
    [FieldValue| RemainingInput2] = RemainingInput1,
    convert_input_into_schema_data_avp_list(RemainingInput2, [{FieldName, FieldValue} | InterimAvpList]).