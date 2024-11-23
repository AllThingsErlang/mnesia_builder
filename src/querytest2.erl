-module(querytest2).
-export([description/0, select/0]).



description() -> "description of query 2".

select() -> io:format("query test 2 completed~n"), {error, not_found}.

