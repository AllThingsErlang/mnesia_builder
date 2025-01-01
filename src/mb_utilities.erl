-module(mb_utilities).
-include("../include/mb.hrl").

-export([find_list_pos/2, create_timestamped_file/1, 
         is_printable_string/1, identify_type_in_string/1, 
         string_to_float/1, string_to_integer/1, string_to_integer/2, 
         string_to_tuple/1, is_comparison/1, is_unquoted_atom/1, parse_input_erlang_terms/1, 
         get_linked_processes/0, is_node_name/1, is_node_name_list/1, is_email/1,
         move_element/3, is_timestamp/1, start_mnesia/0, is_subset/2, ping_nodes/1]).


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
        {FloatValue, []} -> FloatValue;  % Valid float
        _ ->
            case string:to_integer(String) of
                {IntValue, []} -> IntValue * 1.0;  % Convert integer to float
                _ -> {error, {not_a_number, String}}  % Not a number
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
        {IntValue, []} -> IntValue;
        _ -> 
            %% Maybe it is a float. 
            case string:to_float(String) of
                {FloatValue, []} ->
                    if 
                        Round -> round(FloatValue);  % Rounds the float
                        true -> trunc(FloatValue)   % Drops the decimal part
                    end;

                _ -> {error, {not_a_number, String}}
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
is_printable_string(Input) when is_list(Input) ->
    lists:all(fun(Char) -> Char >= 32 andalso Char =< 126 end, Input);

is_printable_string(_) -> false.


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
% Returns: {ok, File} | {error, Reason}
%-------------------------------------------------------------
create_timestamped_file(Path) when is_list(Path) ->
    
    case filelib:ensure_path(Path) of 
        ok -> 
            % Get the current date and time
            {{Year, Month, Day}, {Hour, Min, Sec}} = calendar:now_to_datetime(erlang:timestamp()),
            
            % Format the timestamp as "YYYYMMDD_HHMM"
            Timestamp = io_lib:format("~4..0B~2..0B~2..0B_~2..0B~2..0B~2..0B.csv", [Year, Month, Day, Hour, Min, Sec]),
            
            % Create the filename
            Filename = lists:concat(["output", Timestamp]),
            
            % Open the file for writing
            file:open(Path ++ "/" ++ Filename, [write]);

        {error, Reason} -> {error, Reason}
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



%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
is_unquoted_atom(Atom) when is_atom(Atom) ->
    AtomString = atom_to_list(Atom),
    lists:all(fun is_valid_unquoted_char/1, AtomString)
    andalso is_lowercase_start(AtomString);

is_unquoted_atom(_) -> false.

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
is_valid_unquoted_char(Char) ->
    (Char >= $a andalso Char =< $z) orelse
    (Char >= $0 andalso Char =< $9) orelse
    Char =:= $@ orelse
    Char =:= $_.

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
is_lowercase_start([First | _]) -> First >= $a andalso First =< $z;
is_lowercase_start([]) -> false.


%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
parse_input_erlang_terms(Input) ->
    case erl_scan:string(Input) of
        {ok, Tokens, _} ->
            case erl_parse:parse_exprs(Tokens) of
                {ok, Exprs} ->
                    {ok, lists:map(fun (Expr) ->
                        case erl_eval:expr(Expr, []) of
                            {value, Term, _} -> Term
                        end
                    end, Exprs)};
                {error, Reason} -> {error, Reason}
            end;
        {error, Reason, _} -> {error, Reason}
    end.


%-------------------------------------------------------------
%-------------------------------------------------------------
get_linked_processes() ->
    case erlang:process_info(self(), links) of
        {links, LinkList} ->
            LinkList;
        undefined ->
            [] % Or handle the case where the process doesn't exist
    end.
    


%-------------------------------------------------------------
%   
%-------------------------------------------------------------
-spec is_node_name(atom()) -> boolean().
%-------------------------------------------------------------
is_node_name(Value) when is_atom(Value) -> string:find(atom_to_list(Value), "@") /= nomatch;

is_node_name(_) -> false.

%-------------------------------------------------------------
%   
%-------------------------------------------------------------
-spec is_email(string()) -> boolean().
%-------------------------------------------------------------
is_email(Value) when is_list(Value) -> 
    case is_printable_string(Value) of 
        true -> 
            case (is_valid_email_chars(Value)) of 
                true -> 
                    case string:find(Value, "@") of 
                        nomatch -> false; % no @ not an email
                        "@" -> false; % only one @ at end of email, no domain name
                        Remainder1 -> 
                            case string:find(Remainder1, ".") of 
                                nomatch -> false; % not a valid domain name
                                "." -> false; % only one . at the end, invalid domain name
                                Remainder2 -> 
                                    case string:find(Remainder2, "@") of 
                                        nomatch -> % only one @, good
                                            LastAsciiValue = lists:last(Remainder2),
                                            case [LastAsciiValue] of
                                                "." -> false;   % ends with a .
                                                _ -> true % valid email address
                                            end;
                                        _ -> false
                                    end
                            end 
                    end;

                _ -> false
            end;
        false -> false 
    end;

is_email(_) -> false.


is_valid_email_chars(String) ->
    ValidChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789@._-",
    lists:all(fun(Char) -> lists:member(Char, ValidChars) end, String).

%-------------------------------------------------------------
%   
%-------------------------------------------------------------
-spec is_node_name_list(list()) -> boolean().
%-------------------------------------------------------------
is_node_name_list([]) -> true;
is_node_name_list([Next | T]) ->
    case is_node_name(Next) of 
        true -> is_node_name_list(T);
        false -> false 
    end;

is_node_name_list(_) -> false.

%-------------------------------------------------------------
%   
%-------------------------------------------------------------
-spec move_element(list(), integer(), integer()) -> list().
%-------------------------------------------------------------
move_element(List, N, M) ->
    % Step 1: Remove the element at position N
    {Element, Rest} = pop_at(List, N),
    
    % Step 2: Insert the element at position M
    insert_at(Rest, M, Element).

pop_at(List, N) ->
    {Prefix, [Element | Suffix]} = lists:split(N - 1, List), % Split at N-1
    {Element, Prefix ++ Suffix}.

insert_at(List, M, Element) ->
    {Prefix, Suffix} = lists:split(M - 1, List), % Split at M-1
    Prefix ++ [Element] ++ Suffix.


%-------------------------------------------------------------
%   
%-------------------------------------------------------------
-spec is_timestamp(tuple()) -> boolean().
%-------------------------------------------------------------
is_timestamp(TimeStamp) -> 
    case TimeStamp of 
        {{Year, Month, Day}, {Hour, Minutes, Seconds}} -> 
            is_integer(Year) andalso 
            is_integer(Month) andalso
            is_integer(Day) andalso
            is_integer(Hour) andalso
            is_integer(Minutes) andalso
            is_integer(Seconds);

        _ -> io:format("no match~n"), false 
    end.

%-------------------------------------------------------------
%   
%-------------------------------------------------------------
-spec start_mnesia() -> ok | mb_error().
%-------------------------------------------------------------
start_mnesia() ->
    case application:start(mnesia) of 
        ok -> ok;
        {error,{already_started,mnesia}} -> ok;
        {error, Reason} -> {error, Reason}
    end.

%-------------------------------------------------------------
%   
%-------------------------------------------------------------
-spec is_subset(list(), list()) -> ok | mb_error().
%-------------------------------------------------------------
is_subset(List1, List2) ->
    lists:all(fun(Elem) -> lists:member(Elem, List2) end, List1).



%-------------------------------------------------------------
%   
%-------------------------------------------------------------
-spec ping_nodes(list()) -> boolean().
%-------------------------------------------------------------
ping_nodes(NodesList) ->
    lists:all(fun(Node) -> net_adm:ping(Node) == pong end, NodesList).
