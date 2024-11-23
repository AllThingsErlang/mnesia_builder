-module(querytest1).
-export([description/0, select/0]).



description() -> "description of query 1".

select() -> io:format("query test 1 completed~n"), {error, not_found}.

