-module(test).
-export([read_tuple/0]).


-spec read_tuple() -> tuple() | {error, term()}.
read_tuple() ->
    Input = io:get_line("Enter tuple: "),
    case erl_scan:string(Input ++ ".") of  % Add '.' to complete the term for parsing
        {ok, Tokens, _} ->
            case erl_parse:parse_exprs(Tokens) of
                {ok, [Parsed]} ->
                    case erl_eval:expr(Parsed, []) of
                        {value, Tuple, _} when is_tuple(Tuple) -> 
                            Tuple;  % Successfully evaluated as a tuple
                        _ -> 
                            {error, "Input is not a tuple"}
                    end;
                {ok, _} -> 
                    {error, "Input is not a tuple"};
                {error, Reason} -> 
                    {error, Reason}
            end;
        {error, Reason, _} -> 
            {error, Reason}
    end.

