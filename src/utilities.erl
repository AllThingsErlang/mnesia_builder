-module(utilities).

-export([find_list_pos/2, create_timestamped_file/1, 
         is_printable_string/1, identify_type_in_string/1, 
        string_to_float/1, string_to_integer/1, string_to_integer/2, string_to_tuple/1, is_comparison/1]).


find_list_pos(_Field, []) -> 0;
find_list_pos(Field, List) -> find_list_pos(Field, List, 1).

find_list_pos(_Field, [], _Pos) -> 0;
find_list_pos(Field, [Field | _Remaining], Pos) -> Pos;
find_list_pos(Field, [_Next | Remaining], Pos) -> find_list_pos(Field, Remaining, Pos+1).



%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
string_to_float(String) ->
    case string:to_float(String) of
        {ok, FloatValue} -> {ok, FloatValue};  % Valid float
        {error, _} ->
            case string:to_integer(String) of
                {ok, IntValue} -> {ok, IntValue * 1.0};  % Convert integer to float
                {error, Reason} -> {error, Reason}  % Not a number
            end
    end.


%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
string_to_integer(String) -> string_to_integer(String, false).

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
string_to_integer(String, Round) ->

    case string:to_integer(String) of
        {ok, IntValue} -> {ok, IntValue};
        {error, Reason} -> 
            %% Maybe it is a float. 
            case string:to_float(String) of
                {ok, FloatValue} ->
                    if 
                        Round -> {ok, round(FloatValue)};  % Rounds the float
                        true -> {ok, trunc(FloatValue)}   % Drops the decimal part
                    end;

                _ -> {error, Reason}
            end
    end.

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
string_to_tuple(String) ->

    % Add '.' to complete the term for parsing
    case erl_scan:string(String ++ ".") of  

        {ok, Tokens, _} ->
            case erl_parse:parse_exprs(Tokens) of
                {ok, [Parsed]} ->
                    case erl_eval:expr(Parsed, []) of
                        {value, Tuple, _} when is_tuple(Tuple) -> {ok, Tuple};
                        _ -> {error, not_a_tuple}
                    end;
                
                {ok, _} -> {error, not_a_tuple};
                {error, Reason} -> {error, Reason}
            end;
        {error, Reason, _} -> {error, Reason}
    end.
%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
is_printable_string(Input) ->
    lists:all(fun(Char) -> Char >= 32 andalso Char =< 126 end, Input).


%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
identify_type_in_string(String) ->

    case string:to_integer(String) of
        {ok, _} -> integer;
        _ ->
            case string:to_float(String) of
                {ok, _} -> float;
                _ -> string
            end 
    end.

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
create_timestamped_file(Path) when is_list(Path) ->
    
    filelib:ensure_dir(Path),

    % Get the current date and time
    {{Year, Month, Day}, {Hour, Min, Sec}} = calendar:now_to_datetime(erlang:timestamp()),
    
    % Format the timestamp as "YYYYMMDD_HHMM"
    Timestamp = io_lib:format("~4..0B~2..0B~2..0B_~2..0B~2..0B~2..0B.csv", [Year, Month, Day, Hour, Min, Sec]),
    
    % Create the filename
    Filename = lists:concat(["output", Timestamp]),
    
    % Open the file for writing
    case file:open(Path ++ "/" ++ Filename, [write]) of
        {ok, File} ->
            io:format("File ~s created successfully.~n", [Filename]),
            {ok, File};
        {error, Reason} ->
            io:format("Failed to create file: ~s~n", [Filename]),
            {error, Reason}
    end.

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
is_comparison(Operator) ->

    case Operator of
        '==' -> true;
        '=:=' -> true;
        '>=' -> true;
        '=<' -> true;
        '/=' -> true;
        _ -> false
    end.

