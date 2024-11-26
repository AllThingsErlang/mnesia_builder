-module(query_db).
-import(mnesia, [transaction/1]).
-import(utilities, [find_list_pos/2]).

-include("../include/schemas.hrl").

-export([read/2, select/4, select_or/6, select_and/6, build_matchhead/1]).


%-------------------------------------------------------------
% Function: read
% Purpose:  Reads a record from the the specified table
% Returns:  {ok | Record} | {error, Reason}
%-------------------------------------------------------------
read(Table, Key) ->

    Fun = fun() -> mnesia:read({Table, Key}) end,

    case mnesia:transaction(Fun) of
        {atomic, []} -> {error, not_found};
        {atomic, [Record]} -> {ok, Record};
        {aborted, Reason} -> {error, Reason}
    end.



%-------------------------------------------------------------
% Function: select
% Purpose:  Selects all the tuples that satisfy the specifications
% Returns:  {ok, List} | {error, Reason}
%-------------------------------------------------------------
select(Table, FieldName, Operator, Value) -> 
    MatchHead = build_matchhead(Table),

    Fun = fun() -> 

        FieldPos = get_field_position(Table, FieldName),

        % we increment the position by 1 since the matchhead
        % contains the table schema name, so everything has
        % shifted by one.
        Guard = [{Operator, element(FieldPos + 1, MatchHead), Value}],
        mnesia:select(Table, [{MatchHead, Guard, ['$_']}])
    end,

    case mnesia:transaction(Fun) of
        {atomic, []} -> {error, not_found};
        {atomic, List} -> {ok, List};
        {aborted, Reason} -> {error, Reason}
    end.

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
select_or(Table, FieldName, Operator1, Value1, Operator2, Value2) -> 
    
    MatchHead = build_matchhead(Table),

    Fun = fun() -> 
        FieldPos = get_field_position(Table, FieldName),

        % we increment the position by 1 since the matchhead
        % contains the table schema name, so everything has
        % shifted by one.
        %GuardTest = [{orelse, {'>', '$1', 3}, {'<', '$1', 5}}],
        
        Guard = [{'orelse', {Operator1, element(FieldPos + 1, MatchHead), Value1}, 
                          {Operator2, element(FieldPos + 1, MatchHead), Value2}}],
        mnesia:select(Table, [{MatchHead, Guard, ['$_']}])
    end,

    case mnesia:transaction(Fun) of
        {atomic, []} -> {error, not_found};
        {atomic, List} -> {ok, List};
        {aborted, Reason} -> {error, Reason}
    end.


%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
select_and(Table, FieldName, Operator1, Value1, Operator2, Value2) -> 
    MatchHead = build_matchhead(Table),

    Fun = fun() -> 
        FieldPos = get_field_position(Table, FieldName),

        % we increment the position by 1 since the matchhead
        % contains the table schema name, so everything has
        % shifted by one.
        Guard = [{'andalso', {Operator1, element(FieldPos + 1, MatchHead), Value1}, 
                          {Operator2, element(FieldPos + 1, MatchHead), Value2}
                 }],
        mnesia:select(Table, [{MatchHead, Guard, ['$_']}])
    end,

    case mnesia:transaction(Fun) of
        {atomic, []} -> {error, not_found};
        {atomic, List} -> {ok, List};
        {aborted, Reason} -> {error, Reason}
    end.


%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
get_field_position(Table, FieldName) ->

    case Table of
        table_1 -> Fields = record_info(fields, table_1);
        table_2 -> Fields = record_info(fields, table_2)
    end,

    find_list_pos(FieldName, Fields).



%-------------------------------------------------------------
% Function: build_matchhead
% Purpose:  build the matchhead for the subscriber table to be 
%           used by select calls
% Returns:  Tuple
%-------------------------------------------------------------
build_matchhead(Table) -> list_to_tuple([Table | build_matchhead_list(Table)]).


build_matchhead_list(Table) ->
    
    case Table of
        table_1 -> FieldCount = length(record_info(fields, table_1));
        table_2 -> FieldCount = length(record_info(fields, table_2))
    end,

    build_matchhead_list(FieldCount, []).

build_matchhead_list(0, MatchHeadList) -> MatchHeadList;
build_matchhead_list(N, MatchHeadList) -> 
    build_matchhead_list(N-1, [list_to_atom("$" ++ integer_to_list(N)) | MatchHeadList]).




                      