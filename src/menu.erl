-module(menu).
-include("../include/coverage_analysis.hrl").
-export([start/0]).


start() -> 
    LoadedModules = load_query_modules(),
    clear_screen(),
    display_menu(main, LoadedModules).

%-------------------------------------------------------------
% Function: menu
% Purpose:  Displays the user menu
% Returns:  
%-------------------------------------------------------------
display_menu(Menu, LoadedModules) ->

    case Menu of

        main ->
            io:format("~n~n"),
            io:format("-------------------------------------~n"),
            io:format("   M A I N   M E N U~n"),
            io:format("-------------------------------------~n"),
            io:format("   - admin: administer db~n"),
            io:format("   - data:  manage data~n"),
            io:format("   - query: run queries~n"),
            io:format("-------------------------------------~n"),
            io:format("   - help:    list menu commands~n"),
            io:format("   - exit:  exit shell~n"),
            io:format("-------------------------------------~n"),
            io:format("~n~n");

        admin ->
            io:format("~n~n"),
            io:format("-------------------------------------~n"),
            io:format("   D B   A D M I N I S T R A T I O N~n"),
            io:format("-------------------------------------~n"),
            io:format("   - tables:  list tables~n"),
            io:format("   - start:   start the db~n"),
            io:format("   - stop:    stop the db~n"),
            io:format("   - size:    get table size~n"),
            io:format("-------------------------------------~n"),
            io:format("   - help:    list menu commands~n"),
            io:format("   - return:  return to previous menu~n"),
            io:format("   - exit:    exit shell~n"),
            io:format("-------------------------------------~n"),
            io:format("~n~n");

        data ->
            mnesia_running_banner_warning(),
            
            io:format("~n~n"),
            io:format("-------------------------------------~n"),
            io:format("   D A T A   M A N A G E M E N T~n"),
            io:format("-------------------------------------~n"),
            io:format("   - clear:   clear all data in table~n"),
            io:format("   - load:    load data from csv file~n"),
            io:format("   - size:    get table size~n"),
            io:format("   - fields:  display field names~n"),
            io:format("   - del:     delete a record~n"),
            io:format("-------------------------------------~n"),
            io:format("   - help:    list menu commands~n"),
            io:format("   - return:  return to previous menu~n"),
            io:format("   - exit:    exit shell~n"),
            io:format("-------------------------------------~n"),
            io:format("~n~n");

        query ->
            mnesia_running_banner_warning(),
            
            io:format("~n~n"),
            io:format("-------------------------------------~n"),
            io:format("   Q U E R I E S~n"),
            io:format("-------------------------------------~n"),
            io:format("   - read:    read record based on subscriber id~n"),
            io:format("   - fields:  display field names~n"),
            io:format("   - oper:    list query operators~n"),
            io:format("   - q:       query table using 1 criteria~n"),
            io:format("   - qor:     query table using 2 criteria, OR operator~n"),
            io:format("   - qand:    query table using 2 criteria, AND operator~n"),
            io:format("   - qdl      prebuilt queries, dynamically loaded~n"),
            io:format("-------------------------------------~n"),
            io:format("   - help:    list menu commands~n"),
            io:format("   - return:  return to previous menu~n"),
            io:format("   - exit:    exit shell~n"),
            io:format("-------------------------------------~n"),
            io:format("~n~n")
    end,

    prompt(Menu, LoadedModules).


prompt(main, LoadedModules) -> 
    io:format("~nmain menu> "),
    Input = get_user_input(),
    process_main_menu_input(Input, LoadedModules);

prompt(admin, LoadedModules) -> 
    io:format("~nadmin> "),
    Input = get_user_input(),
    process_admin_menu_input(Input, LoadedModules);

prompt(data, LoadedModules) -> 
    io:format("~ndata> "),
    Input = get_user_input(),
    process_data_menu_input(Input, LoadedModules);

prompt(query, LoadedModules) -> 
    io:format("~nquery> "),
    Input = get_user_input(),
    process_query_menu_input(Input, LoadedModules).



%-------------------------------------------------------------
% Function: 
% Purpose: 
% Returns:  
%-------------------------------------------------------------
process_main_menu_input(Input, LoadedModules) ->
   
   MyMenu = main,

    case Input of
        "admin" -> clear_screen(), display_menu(admin, LoadedModules);
        "data" -> clear_screen(), display_menu(data, LoadedModules);
        "query" -> clear_screen(), display_menu(query, LoadedModules);
        "help" -> display_menu(MyMenu, LoadedModules);
        "exit" -> ok;
        "" -> prompt(MyMenu, LoadedModules);
        _ -> 
            io:format("invalid input~n"),
            prompt(MyMenu, LoadedModules)
    end.


%-------------------------------------------------------------
% Function: process_admin_menu_input
% Purpose:  
% Returns:  
%-------------------------------------------------------------
process_admin_menu_input(Input, LoadedModules) ->

    MyMenu = admin,

    case Input of
        "tables" ->
            display_tables(),
            prompt(MyMenu, LoadedModules);
            
        "start" -> 
            Result = manage_db:start(),
            io:format("Result: ~w~n", [Result]),
            prompt(MyMenu, LoadedModules);

        "stop" -> 
            Result = manage_db:stop(),
            io:format("Result: ~w~n", [Result]),
            prompt(MyMenu, LoadedModules);

        "size" -> 
            case mnesia:system_info(running_db_nodes) of
                [] -> io:format("must start mnesia db~n");
                _ ->
                    Result = manage_db:size(), 
                    io:format("Result: ~w~n", [Result])
            end,

            prompt(MyMenu, LoadedModules);

        "help" -> display_menu(MyMenu, LoadedModules);
        "return" -> prompt(main, LoadedModules);
        "exit" -> ok;
        "" -> prompt(MyMenu, LoadedModules);
        _ -> 
            io:format("invalid input~n"),
            prompt(MyMenu, LoadedModules)
    end.


%-------------------------------------------------------------
% Function: 
% Purpose:   
% Returns:  
%-------------------------------------------------------------
process_data_menu_input(Input, LoadedModules) ->

    MyMenu = data,

    case Input of
        "clear" -> 
            mnesia:clear_table(subscribers),
            io:format("Data cleared~n~n"),
            prompt(MyMenu, LoadedModules);

        "load" -> 
            io:format("File: "),
            File = get_user_input(),
            case load_from_csv(File) of
                ok -> io:format("Data loaded~n~n");
                {error, Reason} -> io:format("~s~n~n", [Reason])
            end,

            prompt(MyMenu, LoadedModules);

        "size" -> 
            Result = manage_db:size(), 
            io:format("Result: ~w~n", [Result]),
            prompt(MyMenu, LoadedModules);
        
        "fields" ->
            display_field_names(),
            prompt(MyMenu, LoadedModules);

        "del" -> 
            io:format("Subscriber ID: "),
            SubId = get_user_input(),
            Result = queries:delete(SubId),

            case Result of
                ok -> io:format("Delted~n~n");
                {_, Other} -> io:format("~w~n~n", [Other])
            end,

            prompt(MyMenu, LoadedModules);
        
        "help" -> display_menu(MyMenu, LoadedModules);
        "return" -> prompt(main, LoadedModules);
        "exit" -> ok;
        "" -> prompt(MyMenu, LoadedModules);
        _ -> 
            io:format("invalid input~n"),
            prompt(MyMenu, LoadedModules)
    end.

%-------------------------------------------------------------
% Function: query_menu
% Purpose:  Displays the db admin menu
% Returns:  
%-------------------------------------------------------------
process_query_menu_input(Input, LoadedModules) ->

    MyMenu = query,
    
    case Input of
        "read" -> 
            io:format("Subscriber ID: "),
            SubId = get_user_input(),
            Result = queries:read(SubId),

            case Result of
                {ok, Record} -> print_subscriber_record(Record);
                {_, Other} -> io:format("~w~n", [Other])
            end,

            prompt(MyMenu, LoadedModules);

        "fields" ->
            display_field_names(),
            prompt(MyMenu, LoadedModules);

        "oper" ->
            io:format("   ==~n"),
            io:format("   =:=~n"),
            io:format("   >=~n"),
            io:format("   =<~n"),
            io:format("   /=~n"),
            io:format("~n~n"),

            prompt(MyMenu, LoadedModules);
            
        "q" -> 
            io:format("Field: "),
            Field = list_to_atom(get_user_input()),

            case (lists:member(Field, record_info(fields, subscribers))) of

                true ->

                    io:format("Operator: "),
                    Oper = list_to_atom(get_user_input()),

                    io:format("Value: "),
                    Value = get_user_input(),

                    case get_field_type(Field) of
                        string -> ConvertedValue = Value;
                        float -> ConvertedValue = utilities:string_to_float(Value)
                    end,

                    QueryOutput = queries:select(Field, Oper, ConvertedValue), 
                    process_query_output(QueryOutput);

                false ->

                    io:format("invalid field name~n")
            end,

            prompt(MyMenu, LoadedModules);

        "qor" -> 
            io:format("Field: "),
            Field = list_to_atom(get_user_input()),

            case (lists:member(Field, record_info(fields, subscribers))) of

                true ->
                    io:format("Operator 1: "),
                    Oper1 = list_to_atom(get_user_input()),

                    io:format("Value 1: "),
                    Value1 = get_user_input(),

                    io:format("Operator 2: "),
                    Oper2 = list_to_atom(get_user_input()),

                    io:format("Value 2: "),
                    Value2 = get_user_input(),

                    case get_field_type(Field) of
                        string -> 
                            ConvertedValue1 = Value1,
                            ConvertedValue2 = Value2;

                        float -> 
                            ConvertedValue1 = utilities:string_to_float(Value1),
                            ConvertedValue2 = utilities:string_to_float(Value2)
                    end,

                    QueryOutput = queries:select_or(Field, Oper1, ConvertedValue1, Oper2, ConvertedValue2), 
                    process_query_output(QueryOutput);

                false ->
                    io:format("invalid field name~n")
            end,

            prompt(MyMenu, LoadedModules);

        "qand" -> 
            io:format("Field: "),
            Field = list_to_atom(get_user_input()),

            case (lists:member(Field, record_info(fields, subscribers))) of

                true ->
                    io:format("Operator 1: "),
                    Oper1 = list_to_atom(get_user_input()),

                    io:format("Value 1: "),
                    Value1 = get_user_input(),

                    io:format("Operator 2: "),
                    Oper2 = list_to_atom(get_user_input()),

                    io:format("Value 2: "),
                    Value2 = get_user_input(),

                    case get_field_type(Field) of
                        string -> 
                            ConvertedValue1 = Value1,
                            ConvertedValue2 = Value2;

                        float -> 
                            ConvertedValue1 = utilities:string_to_float(Value1),
                            ConvertedValue2 = utilities:string_to_float(Value2)
                    end,

                    QueryOutput = queries:select_and(Field, Oper1, ConvertedValue1, Oper2, ConvertedValue2), 
                    process_query_output(QueryOutput);

                false ->
                    io:format("invalid field name~n")
            end,

            prompt(MyMenu, LoadedModules);


        "qdl" ->
            
            LoadedModulesCount = length(LoadedModules),

            case LoadedModulesCount of
                0 -> 
                    io:format("No dynamically loaded queries found.~n");
                
                _ ->

                    display_loaded_modules(LoadedModules),
                    io:format("~nSelect query number or 0 to return: "),
                    QueryNumberString = get_user_input(),
                    QueryNumber = list_to_integer(QueryNumberString),

                    if 
                        QueryNumber == 0 ->
                            ok;

                        QueryNumber > LoadedModulesCount; QueryNumber < 0 ->
                            io:format("invalid query number~n");

                        true ->
                            SelectedModule = lists:nth(QueryNumber, LoadedModules),
                            QueryOutput = SelectedModule:select(),
                            process_query_output(QueryOutput)
                    end
            end,

            prompt(MyMenu, LoadedModules);

        "help" -> display_menu(MyMenu, LoadedModules);
        "return" -> prompt(main, LoadedModules);
        "exit" -> ok;
        "" -> prompt(MyMenu, LoadedModules);
        _ -> 
            io:format("invalid input~n"),
            prompt(MyMenu, LoadedModules)
    end.



%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns:  
%-------------------------------------------------------------
print_subscriber_record(Record) ->
    {subscribers,
     Subscriber_id,
     Nag_id,
     Customer_name,
     Device_type, 
     Total_bad_gb_down,
     Total_gb_down, 
     Total_bad_periods, 
     Total_periods, 
     Total_bad_days,
     Total_days_active,
     Bad_period_pctg,
     Bad_period_pctg_bucket,
     City, 
     Province,
     Postal_code} = Record,


     io:format("~n~n"),
     io:format("        subscriber_id            ~s~n", [Subscriber_id]),
     io:format("        nag_id                   ~s~n", [Nag_id]),
     io:format("        customer_name            ~s~n", [Customer_name]),
     io:format("        device_type              ~s~n", [Device_type]),
     io:format("        total_bad_gb_down        ~w~n", [Total_bad_gb_down]),
     io:format("        total_gb_down            ~w~n", [Total_gb_down]),
     io:format("        total_bad_periods        ~w~n", [Total_bad_periods]),
     io:format("        total_periods            ~w~n", [Total_periods]),
     io:format("        total_bad_days           ~w~n", [Total_bad_days]),
     io:format("        total_days_active        ~w~n", [Total_days_active]),
     io:format("        bad_period_pctg          ~w~n", [Bad_period_pctg]),
     io:format("        bad_period_pctg_bucket   ~w~n", [Bad_period_pctg_bucket]),
     io:format("        city                     ~s~n", [City]),
     io:format("        province                 ~s~n", [Province]),
     io:format("        postal_code              ~s~n~n", [Postal_code]).




%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
write_query_result_to_file(Result) ->

    case Result of
        {ok, Records} -> write_subscribers_list_of_records_to_file(Records);
        Records -> write_subscribers_list_of_records_to_file(Records)
    end.


%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns:  
%-------------------------------------------------------------
write_subscribers_list_of_records_to_file(Records) ->

    case create_csv_file() of
        {ok, File} ->
            write_subscribers_list_of_records_to_file(File, Records);
        {error, Reason} -> {error, Reason}
    end.


write_subscribers_list_of_records_to_file(File, []) -> 
    file:close(File),
    ok;
write_subscribers_list_of_records_to_file(File, [Next|Remaining]) ->
        
    Record = convert_subscribers_tuple_record_to_list_record(Next),

    write_csv_record(File, Record),
    write_subscribers_list_of_records_to_file(File, Remaining).


%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns:  
%-------------------------------------------------------------
create_csv_file() ->
    case utilities:create_timestamped_file("../reports") of
        {ok, File} -> 
            write_csv_header(File, record_info(fields, subscribers)),
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
write_csv_record(File, [subscribers | Remaining]) -> write_csv_record(File, Remaining);
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
convert_subscribers_tuple_record_to_list_record(Record) ->
    L1 = tuple_to_list(Record),
    case L1 of
        {subscribers, L2} -> L2;
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
display_field_names() -> 
    io:format("~n"),
    Fields = record_info(fields, subscribers),
    display_field_names(Fields).

display_field_names([]) -> io:format("~n");
display_field_names([Next | Remaining]) -> 
    io:format("   ~w~n", [Next]),
    display_field_names(Remaining).


%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
display_tables() ->

    Tables = mnesia:system_info(tables),
    io:format("~n~n"),
    lists:foreach(fun(Table) -> io:format("   ~p~n", [Table]) end, Tables).



mnesia_running_banner_warning() ->

    case mnesia:system_info(running_db_nodes) of
        [] -> 
            io:format("*************************************************~n"),
            io:format("   Must start mnesia db before using this menu~n"),
            io:format("*************************************************~n~n");
        _ -> ok
    end.


%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
load_query_modules() ->
    % Find all .beam files that start with "query" in the current directory
    Files = filelib:wildcard("query*.beam"),
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
display_loaded_modules([]) -> ok;
display_loaded_modules(LoadedModules) -> display_loaded_modules(lists:reverse(LoadedModules), 1).

display_loaded_modules([], _) -> ok;
display_loaded_modules([Next | Remaining], N) ->

    io:format("   ~3w ~15w  ~s~n", [N, Next, Next:description()]),
    display_loaded_modules(Remaining, N+1).