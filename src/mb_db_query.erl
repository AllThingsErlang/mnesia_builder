-module(mb_db_query).
-include("../include/mb.hrl").

-import(mnesia, [transaction/1]).
-import(utilities, [find_list_pos/2]).


-export([read/3, select/5, select_or/7, select_and/7, build_matchhead/2]).


%-------------------------------------------------------------
% Reads from the table SchemaName with key = Key. The table must
% have been created.
%-------------------------------------------------------------
-spec read(mb_schema_name(), term(), mb_ssg()) -> list() | mb_error().
%-------------------------------------------------------------
read(SchemaName, Key, SSG) ->

    case mb_ssg:is_schema(SchemaName, SSG) of 
        true -> 
            Fun = fun() -> mnesia:read({SchemaName, Key}) end,

            case mnesia:transaction(Fun) of
                {atomic, Result} -> Result;
                {aborted, Reason} -> {error, Reason}
            end;

        false -> {error, {invalid_schema_name, SchemaName}}
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
-spec select(atom(), atom(), atom(), term(), mb_ssg()) -> list() | mb_error().
%-------------------------------------------------------------
select(SchemaName, FieldName, Operator, Value, SSG) -> 
    MatchHead = build_matchhead(SchemaName, SSG),

    Fun = fun() -> 

        FieldPos = mb_ssg:field_position(FieldName, SchemaName, SSG),

        % we increment the position by 1 since the matchhead
        % contains the table get_schema name, so everything has
        % shifted by one.
        Guard = [{Operator, element(FieldPos + 1, MatchHead), Value}],
        mnesia:select(SchemaName, [{MatchHead, Guard, ['$_']}])
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
-spec select_or(atom(), atom(), atom(), term(), atom(), term(), mb_ssg()) -> list() | mb_error().
%-------------------------------------------------------------
select_or(SchemaName, FieldName, Operator1, Value1, Operator2, Value2, SSG) -> 
    
    MatchHead = build_matchhead(SchemaName, SSG),

    Fun = fun() -> 
        FieldPos = mb_ssg:field_position(FieldName, SchemaName, SSG),

        % we increment the position by 1 since the matchhead
        % contains the table get_schema name, so everything has
        % shifted by one.
        %GuardTest = [{orelse, {'>', '$1', 3}, {'<', '$1', 5}}],
        
        Guard = [{'orelse', {Operator1, element(FieldPos + 1, MatchHead), Value1}, 
                          {Operator2, element(FieldPos + 1, MatchHead), Value2}}],
        mnesia:select(SchemaName, [{MatchHead, Guard, ['$_']}])
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
-spec select_and(atom(), atom(), atom(), term(), atom(), term(), mb_ssg()) -> list() | mb_error().
%-------------------------------------------------------------
select_and(SchemaName, FieldName, Operator1, Value1, Operator2, Value2, SSG) -> 
    MatchHead = build_matchhead(SchemaName, SSG),

    Fun = fun() -> 
        FieldPos = mb_ssg:field_position(FieldName, SchemaName, SSG),

        % we increment the position by 1 since the matchhead
        % contains the table get_schema name, so everything has
        % shifted by one.
        Guard = [{'andalso', {Operator1, element(FieldPos + 1, MatchHead), Value1}, 
                          {Operator2, element(FieldPos + 1, MatchHead), Value2}
                 }],
        mnesia:select(SchemaName, [{MatchHead, Guard, ['$_']}])
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
build_matchhead(SchemaName, SSG) -> list_to_tuple([SchemaName | build_matchhead_list(SchemaName, SSG)]).


build_matchhead_list(SchemaName, SSG) ->

    FieldCount = mb_ssg:field_count(SchemaName, SSG),
    build_matchhead_list_next(FieldCount, []).

build_matchhead_list_next(0, MatchHeadList) -> MatchHeadList;
build_matchhead_list_next(N, MatchHeadList) -> 
    build_matchhead_list_next(N-1, [list_to_atom("$" ++ integer_to_list(N)) | MatchHeadList]).




                      