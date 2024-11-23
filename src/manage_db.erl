-module(manage_db).
-include("../include/coverage.hrl").

-export([install/0, start/0, stop/0, size/0]).

install() ->
    mnesia:create_schema([node()]),
    mnesia:start(),
    mnesia:create_table(subscribers, [
        {disc_copies, [node()]},
        {attributes, record_info(fields, subscribers)},
        {type, set}
    ]),
    mnesia:stop(),
    ok.


start() ->
    mnesia:start(),
    mnesia:wait_for_tables([subscribers], 10000).
 
stop() -> mnesia:stop().



%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
size() -> mnesia:table_info(subscribers, size).


%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
