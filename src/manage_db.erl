-module(manage_db).
-include("../include/schemas.hrl").

-export([install/0, start/0, stop/0, table_sizes/0, table_size/1]).

install() ->
    
    case mnesia:create_schema([node()]) of
        ok -> ok;
        {error, Reason1} -> io:format("failed to create get_schema: ~w~n", [Reason1])
    end,

    mnesia:start(),
    
    case mnesia:create_table(table_1, [
        {disc_copies, [node()]},
        {attributes, record_info(fields, table_1)},
        {type, set}
    ]) of 

        {atomic, _} -> ok;
        {aborted, Reason2} -> io:format("failed to create table_1: ~w~n", [Reason2])
    end,

    case mnesia:create_table(table_2, [
        {disc_copies, [node()]},
        {attributes, record_info(fields, table_2)},
        {type, set}
    ]) of 
        {atomic, _} -> ok;
        {aborted, Reason3} -> io:format("failed to create table_2: ~w~n", [Reason3])
    end,

    ok.


start() ->
    mnesia:start(),
    mnesia:wait_for_tables(schemas:get_tables(), 10000).
 
stop() -> mnesia:stop().



%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
table_size(Table) -> mnesia:table_info(Table, size).

table_sizes() -> table_sizes(schemas:get_tables()).

table_sizes([]) -> [];
table_sizes(Tables) -> table_sizes(Tables, []).

table_sizes([], Sizes) -> lists:reverse(Sizes);
table_sizes([H|T], Sizes) -> table_sizes(T, [{H, table_size(H)} | Sizes]).

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
