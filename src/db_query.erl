-module(db_query).
-import(mnesia, [transaction/1]).
-import(utilities, [find_list_pos/2]).


-export([read/2, select/5, select_or/7, select_and/7, build_matchhead/2]).


%-------------------------------------------------------------
% Reads from the table Table with key = Key. The table must
% have been created.
%-------------------------------------------------------------
-spec read(atom(), term()) -> list() | {error, term()}.
%-------------------------------------------------------------
read(Table, Key) ->

    Fun = fun() -> mnesia:read({Table, Key}) end,

    case mnesia:transaction(Fun) of
        {atomic, Result} -> Result;
        {aborted, Reason} -> {error, Reason}
    end.

%-------------------------------------------------------------
% Selects all tuples that satisfy the criteria: 
%        <FieldName> <Operator> <Value> 
%
% For example,
%       age '>' 30
%       employee_id '==' "N300442"
%
% TODO: 
%    - Validate Operator before using it.
%
%-------------------------------------------------------------
-spec select(atom(), atom(), atom(), term(), map()) -> list() | {error, term()}.
%-------------------------------------------------------------
select(Table, FieldName, Operator, Value, SS) -> 
    MatchHead = build_matchhead(Table, SS),

    Fun = fun() -> 

        FieldPos = db_schemas:field_position(FieldName, Table, SS),

        % we increment the position by 1 since the matchhead
        % contains the table get_schema name, so everything has
        % shifted by one.
        Guard = [{Operator, element(FieldPos + 1, MatchHead), Value}],
        mnesia:select(Table, [{MatchHead, Guard, ['$_']}])
    end,

    case mnesia:transaction(Fun) of
        {atomic, Result} -> Result;
        {aborted, Reason} -> {error, Reason}
    end.

%-------------------------------------------------------------
% Selects all tuples that satisfy the criteria: 
%        
%  (<FieldName> <Operator1> <Value1>) or (<FieldName> <Operator2> <Value2>)
%
% For example:
%
%      (age '<' 18) or (age '>' 45)
%
%-------------------------------------------------------------
-spec select_or(atom(), atom(), atom(), term(), atom(), term(), map()) -> list() | {error, term()}.
%-------------------------------------------------------------
select_or(Table, FieldName, Operator1, Value1, Operator2, Value2, SS) -> 
    
    MatchHead = build_matchhead(Table, SS),

    Fun = fun() -> 
        FieldPos = db_schemas:field_position(FieldName, Table, SS),

        % we increment the position by 1 since the matchhead
        % contains the table get_schema name, so everything has
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
% Selects all tuples that satisfy the criteria: 
%        
%  (<FieldName> <Operator1> <Value1>) and (<FieldName> <Operator2> <Value2>)
%
% For example:
%
%      (age '>' 18) and (age '<' 45)
%
%-------------------------------------------------------------
-spec select_and(atom(), atom(), atom(), term(), atom(), term(), map()) -> list() | {error, term()}.
%-------------------------------------------------------------
select_and(Table, FieldName, Operator1, Value1, Operator2, Value2, SS) -> 
    MatchHead = build_matchhead(Table, SS),

    Fun = fun() -> 
        FieldPos = db_schemas:field_position(FieldName, Table, SS),

        % we increment the position by 1 since the matchhead
        % contains the table get_schema name, so everything has
        % shifted by one.
        Guard = [{'andalso', {Operator1, element(FieldPos + 1, MatchHead), Value1}, 
                          {Operator2, element(FieldPos + 1, MatchHead), Value2}
                 }],
        mnesia:select(Table, [{MatchHead, Guard, ['$_']}])
    end,

    case mnesia:transaction(Fun) of
        {atomic, Result} -> {ok, Result};
        {aborted, Reason} -> {error, Reason}
    end.


%-------------------------------------------------------------
% Function: build_matchhead
% Purpose:  build the matchhead for the subscriber table to be 
%           used by select calls
% Returns:  Tuple
%-------------------------------------------------------------
build_matchhead(Table, SS) -> list_to_tuple([Table | build_matchhead_list(Table, SS)]).


build_matchhead_list(Table, SS) ->

    FieldCount = db_schemas:field_count(Table, SS),
    build_matchhead_list_next(FieldCount, []).

build_matchhead_list_next(0, MatchHeadList) -> MatchHeadList;
build_matchhead_list_next(N, MatchHeadList) -> 
    build_matchhead_list_next(N-1, [list_to_atom("$" ++ integer_to_list(N)) | MatchHeadList]).




                      