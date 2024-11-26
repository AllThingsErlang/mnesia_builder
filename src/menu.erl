-module(menu).
-include("../include/schemas.hrl").
-export([start/0]).


start() -> 
    LoadedModules = load_query_modules(),
    clear_screen(),
    display_menu(LoadedModules).

%-------------------------------------------------------------
% Function: menu
% Purpose:  Displays the user menu
% Returns:  
%-------------------------------------------------------------
display_menu(LoadedModules) ->
    
    io:format("~n~n"),
    io:format("-------------------------------------~n"),
    io:format("   - install: install schemas~n"),
    io:format("   - start:   start the db~n"),
    io:format("   - stop:    stop the db~n"),
    io:format("   - tables:  list tables~n"),
    io:format("   - size:    get table size~n"),
    io:format("   - clear:   clear all data in table~n"),
    io:format("   - load:    load data from csv file~n"),
    io:format("   - fields:  display field names~n"),
    io:format("   - del:     delete a record~n"),
    io:format("   - read:    read record based on subscriber id~n"),
    io:format("   - fields:  display field names~n"),
    io:format("   - oper:    list query operators~n"),
    io:format("   - q:       query table using 1 criteria~n"),
    io:format("   - qor:     query table using 2 criteria, OR operator~n"),
    io:format("   - qand:    query table using 2 criteria, AND operator~n"),
    io:format("   - qdl      prebuilt queries, dynamically loaded~n"),
    io:format("-------------------------------------~n"),
    io:format("   - help:    list menu commands~n"),
    io:format("   - exit:    exit shell~n"),
    io:format("-------------------------------------~n"),
    io:format("~n~n"),

    prompt(LoadedModules).


prompt(LoadedModules) -> 
    io:format("~nmnesia> "),
    Input = get_user_input(),
    process_user_input(Input, LoadedModules).



%-------------------------------------------------------------
% Function: process_admin_menu_input
% Purpose:  
% Returns:  
%-------------------------------------------------------------
process_user_input(Input, LoadedModules) ->

    case string:tokens(Input, " ") of

        [Command] ->

            case Command of
                "install" ->
                    manage_db:install(),
                    prompt(LoadedModules);
                
                "tables" ->
                    process_tables(),
                    prompt(LoadedModules);
                    
                "start" -> 
                    Result = manage_db:start(),
                    io:format("Result: ~w~n", [Result]),
                    prompt(LoadedModules);

                "stop" -> 
                    Result = manage_db:stop(),
                    io:format("Result: ~w~n", [Result]),
                    prompt(LoadedModules);

                "size" -> 
                    process_size([]),
                    prompt(LoadedModules);

                "clear" ->
                    process_clear([]),
                    prompt(LoadedModules);

                "fields" ->
                    process_fields([]),
                    prompt(LoadedModules);

                "oper" ->
                    io:format("   ==~n"),
                    io:format("   =:=~n"),
                    io:format("   >=~n"),
                    io:format("   =<~n"),
                    io:format("   /=~n"),
                    prompt(LoadedModules);

                "qdl" ->
            
                    LoadedModulesCount = length(LoadedModules),

                    case LoadedModulesCount of

                        0 -> 
                            io:format("no dynamically loaded queries found~n");
                        
                        _ ->

                            display_loaded_modules(LoadedModules),
                            io:format("~nSelect query number or 0 to return: "),
                            QueryNumberString = get_user_input(),

                            case schemas:safe_convert_from_string(QueryNumberString, integer) of
                                {ok, QueryNumber} ->

                                    if 
                                        QueryNumber == 0 ->
                                            ok;

                                        QueryNumber > LoadedModulesCount; QueryNumber < 0 ->
                                            io:format("invalid query number~n");

                                        true ->
                                            SelectedModule = lists:nth(QueryNumber, LoadedModules),
                                            QueryOutput = SelectedModule:select(),
                                            process_query_output(QueryOutput)
                                    end;

                                _ -> io:format("invalid input~n")
                            end
                    end,

                    prompt(LoadedModules);

                "help" -> display_menu(LoadedModules);
                "exit" -> ok;

                "" -> prompt(LoadedModules);
                _ -> 
                    io:format("invalid input~n"),
                    prompt(LoadedModules)
            end;

        [Command, Arg1] ->

            case Command of

                "size" -> 
                    process_size(list_to_atom(Arg1)),
                    prompt(LoadedModules);

                "clear" ->
                    process_clear(list_to_atom(Arg1)),
                    prompt(LoadedModules);

                "fields" ->
                    process_fields(list_to_atom(Arg1)),
                    prompt(LoadedModules);

                _ -> 
                    io:format("invalid input~n"),
                    prompt(LoadedModules)
            end;

        [Command, Arg1, Arg2] -> 

            case Command of

                "del" -> 

                    case mnesia:system_info(running_db_nodes) of
                        [] -> io:format("must start mnesia db~n");

                        _ ->

                            % 1. Get the user provided table name and confirm it is valid
                            % 2. Get the user provided key value and convert it 
                            %    based on the expected type and confirm it is valid. 
                            % 3. Execute the command

                            Table = list_to_atom(Arg1),

                            case lists:member(Table, schemas:get_tables()) of
                                true -> 

                                    % Table name is valid
                                    case schemas:safe_convert_from_string(Arg2, schemas:get_key_type(Table)) of
                                        
                                        {ok, Key} ->

                                            % Supplied key value is valid
                                            Result = modify_db:delete(Table, Key),
                                    
                                            case Result of
                                                ok -> io:format("delted~n");
                                                {_, Other} -> io:format("~w~n", [Other])
                                            end;
                                        {error, Reason} -> io:format("delete failed, ~w~n", [Reason])
                                    end;
                                false -> 
                                    io:format("table ~w not recognized ~n", [Table])
                            end
                        end,

                    prompt(LoadedModules);

                "read" -> 

                    % q  Arg1          Arg2     Arg3   
                    % q  <table_name>  <oper>   <value>

                    case mnesia:system_info(running_db_nodes) of
                        [] -> io:format("must start mnesia db~n");

                        _ ->
                            
                            Table = list_to_atom(Arg1),

                            case lists:member(Table, schemas:get_tables()) of
                                true -> 
                                    case schemas:safe_convert_from_string(Arg2, schemas:get_key_type(Table)) of
                                        
                                        {ok, Key} ->
                                            Result = queries:read(Table, Key),

                                            case Result of
                                                {ok, Record} -> print_record(Record);
                                                {_, Other} -> io:format("~w~n", [Other])
                                            end;
                                        {error, Reason} -> io:format("read failed, ~w~n", [Reason])
                                    end;
                                false -> io:format("table ~w not recognized ~n", [Table])
                            end
                    end,

                    prompt(LoadedModules);

                _ ->
                    io:format("invalid input~n"),
                    prompt(LoadedModules)
            end;

        [Command, Arg1, Arg2, Arg3, Arg4] ->

            case Command of 
                
                "q" -> 

                    % q  Arg1          Arg2          Arg3     Arg4 
                    % q  <table_name>  <field_name>  <oper>   <value>

                    case mnesia:system_info(running_db_nodes) of
                        [] -> io:format("must start mnesia db~n");

                        _ ->
                            
                            Table = list_to_atom(Arg1),
                            Field = list_to_atom(Arg2),
                            Oper = list_to_atom(Arg3),
                            
                            case lists:member(Table, schemas:get_tables()) of
                                true -> 
                                    case schemas:is_field(Table, Field) of
                                        true ->
                                            case utilities:is_comparison(Oper) of 
                                                true ->
                                                    case schemas:safe_convert_from_string(Arg4, schemas:get_field_type(Table, Field)) of
                                                        {ok, Value} -> 
                                                            QueryOutput = queries:select(Table, Field, Oper, Value), 
                                                            process_query_output(QueryOutput);

                                                        _ -> io:format("invalid data type~n")
                                                    end;
                                                false -> io:format("not a comparison operator~n")
                                            end;
                                        false -> io:format("not a field in table~n")
                                    end;
                                false -> io:format("table ~w not recognized ~n", [Table])
                            end
                    end,

                    prompt(LoadedModules);

                _ -> 
                    io:format("invalid input~n"),
                    prompt(LoadedModules)

            end;

        [Command, Arg1, Arg2, Arg3, Arg4, Arg5, Arg6] ->

            case Command of 
                
                "qor" ->
                    
                    % qor Arg1         Arg2         Arg3    Arg4     Arg5    Arg6
                    % qor <table_name> <field_name> <oper1> <value1> <oper2> <value2>

                    case mnesia:system_info(running_db_nodes) of
                        [] -> io:format("must start mnesia db~n");

                        _ ->
                            
                            Table = list_to_atom(Arg1),
                            Field = list_to_atom(Arg2),
                            Oper1 = list_to_atom(Arg3),
                            Oper2 = list_to_atom(Arg5),

                            case lists:member(Table, schemas:get_tables()) of
                                true -> 
                                    case schemas:is_field(Table, Field) of
                                        true ->
                                            case (utilities:is_comparison(Oper1) and utilities:is_comparison(Oper2)) of 
                                                true ->
                                                    case schemas:safe_convert_from_string(Arg4, schemas:get_field_type(Table, Field)) of
                                                        {ok, Value1} -> 

                                                            case schemas:safe_convert_from_string(Arg6, schemas:get_field_type(Table, Field)) of
                                                                {ok, Value2} -> 
                                                                    QueryOutput = queries:select_or(Field, Oper1, Value1, Oper2, Value2), 
                                                                    process_query_output(QueryOutput);

                                                                _ -> io:format("invalid data type~n")
                                                            end;
                                                        _ -> io:format("invalid data type~n")
                                                    end;
                                                false -> io:format("not a comparison operator~n")
                                            end;
                                        false -> io:format("not a field in table~n")
                                    end;
                                false -> io:format("table ~w not recognized ~n", [Table])
                            end
                    end,

                    prompt(LoadedModules);

                "qand" ->
                    
                    % qand Arg1         Arg2         Arg3    Arg4     Arg5    Arg6
                    % qand <table_name> <field_name> <oper1> <value1> <oper2> <value2>

                    case mnesia:system_info(running_db_nodes) of
                        [] -> io:format("must start mnesia db~n");

                        _ ->
                            
                            Table = list_to_atom(Arg1),
                            Field = list_to_atom(Arg2),
                            Oper1 = list_to_atom(Arg3),
                            Oper2 = list_to_atom(Arg5),

                            case lists:member(Table, schemas:get_tables()) of
                                true -> 
                                    case schemas:is_field(Table, Field) of
                                        true ->
                                            case (utilities:is_comparison(Oper1) and utilities:is_comparison(Oper2)) of 
                                                true ->
                                                    case schemas:safe_convert_from_string(Arg4, schemas:get_field_type(Table, Field)) of
                                                        {ok, Value1} -> 

                                                            case schemas:safe_convert_from_string(Arg6, schemas:get_field_type(Table, Field)) of
                                                                {ok, Value2} -> 
                                                                    QueryOutput = queries:select_and(Field, Oper1, Value1, Oper2, Value2), 
                                                                    process_query_output(QueryOutput);

                                                                _ -> io:format("invalid data type~n")
                                                            end;
                                                        _ -> io:format("invalid data type~n")
                                                    end;
                                                false -> io:format("not a comparison operator~n")
                                            end;
                                        false -> io:format("not a field in table~n")
                                    end;
                                false -> io:format("table ~w not recognized ~n", [Table])
                            end
                    end,

                    prompt(LoadedModules);

                 _ -> 
                    io:format("invalid input~n"),
                    prompt(LoadedModules)

            end;
        
        _ -> 
            io:format("invalid input~n"),
            prompt(LoadedModules)
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
    case utilities:create_timestamped_file("../reports") of
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
            case write_query_result_to_file(QueryOutput) of
                ok -> io:format("Output written to file~n~n");
                {_, Other} -> io:format("~w~n~n", [Other])
            end;

        {error, not_found} -> io:format("not found~n");
        {error, {badarg, _}} -> io:format("invalid input~n");
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
        utilities:string_to_float(lists:nth(5, Fields)),
        utilities:string_to_float(lists:nth(6, Fields)),
        utilities:string_to_float(lists:nth(7, Fields)),
        utilities:string_to_float(lists:nth(8, Fields)),
        utilities:string_to_float(lists:nth(9, Fields)),
        utilities:string_to_float(lists:nth(10, Fields)),
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



%get_field_type(Position, Fields) ->
%    get_field_type(lists:nth(Position, Fields)).

%-------------------------------------------------------------
% Function: get_field_type
% Purpose:  
% Returns:  
%-------------------------------------------------------------

get_field_type(Field) ->

    case Field of
        subscriber_id -> string;
        nag_id -> string;
        customer_name -> string;
        device_type -> string;
        total_bad_gb_down -> float;
        total_gb_down -> float;
        total_bad_periods -> float;
        total_periods -> float;
        total_bad_days -> float;
        total_days_active -> float;
        bad_period_pctg -> float;
        bad_period_pctg_bucket -> float;
        city -> string;
        province -> string;
        postal_code -> string;
        _ -> uknown
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
process_fields(Table) -> 

    % This function makes a decision, is it all tables or just one.
    % And then calls the proper processor.
    case Table of
        [] -> process_display_fields_all_tables(schemas:get_tables());
        _ -> process_display_fields_one_table(Table)
    end.


%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
process_display_fields_one_table(Table) -> 
    io:format("~w~n", [Table]),

    case Table of
        table_1 -> display_field_names(record_info(fields, table_1));
        table_2 -> display_field_names(record_info(fields, table_2));
        _ -> io:format("table not recognized~n")
    end.

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
process_display_fields_all_tables([]) -> io:format("~n");
process_display_fields_all_tables([H|T]) ->
    process_display_fields_one_table(H),
    process_display_fields_all_tables(T).

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
display_field_names([]) -> io:format("~n");
display_field_names([Next | Remaining]) -> 
    io:format("   ~w~n", [Next]),
    display_field_names(Remaining).


%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
process_tables() ->

    case mnesia:system_info(running_db_nodes) of
        [] -> 
            io:format("must start mnesia db~n");

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
process_size(Table) ->

    case mnesia:system_info(running_db_nodes) of
        [] -> io:format("must start mnesia db~n");
        _ ->

        case Table of
            [] -> display_list_of_tuples_of_pairs(manage_db:table_sizes());
            _ ->

                case lists:member(Table, schemas:get_tables()) of
                    true ->
                        Result = manage_db:table_size(Table), 
                        io:format("~w ~w~n", [Table, Result]);
                    false ->
                        io:format("Table ~w not recognized ~n", [Table])
                end
        end
    end.

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
process_clear(Table) ->

    case mnesia:system_info(running_db_nodes) of
        [] -> io:format("must start mnesia db~n");
        _ ->

        case Table of
            [] -> 
                io:format("clear *all* tables? [y/n]: "),
                Input = get_user_input(),

                case string:to_lower(Input) of
                    "y" -> 
                        {Cleared, NotCleared} = modify_db:clear_all_tables(),
                        io:format("cleared ~w out of ~w~n", [Cleared, Cleared+NotCleared]);

                    _ -> 
                        io:format("aborted~n")
                end;
            
            _ ->

                case lists:member(Table, schemas:get_tables()) of
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
                        io:format("table ~w not recognized ~n", [Table])
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
            io:format("Failed to load module: ~w~n", [ModuleName]),
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
display_loaded_modules([]) -> ok;
display_loaded_modules(LoadedModules) -> display_loaded_modules(lists:reverse(LoadedModules), 1).

display_loaded_modules([], _) -> ok;
display_loaded_modules([Next | Remaining], N) ->

    io:format("   ~3w ~15w  ~s~n", [N, Next, Next:description()]),
    display_loaded_modules(Remaining, N+1).


%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
display_list_of_tuples_of_pairs([]) -> io:format("~n");
display_list_of_tuples_of_pairs([{A, B} | T]) -> 
    io:format("~w ~w~n", [A, B]),
    display_list_of_tuples_of_pairs(T).