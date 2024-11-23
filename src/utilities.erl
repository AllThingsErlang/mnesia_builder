-module(utilities).

-export([find_list_pos/2, create_timestamped_file/1, string_to_float/1]).


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
    case lists:member($.,String) of
        true -> list_to_float(String);
        false -> list_to_float(String ++ ".0")
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



