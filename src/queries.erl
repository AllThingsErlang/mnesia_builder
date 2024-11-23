-module(queries).
%-behaviour(application).
-import(mnesia, [transaction/1, foldl/3]).
-import(utilities, [find_list_pos/2]).

-include("../include/coverage_analysis.hrl").

-export([add/13, delete/1, read/1, select/3, select_or/5, select_and/5]).


%-------------------------------------------------------------
% Function: add
% Purpose:  Adds a record to the subscribers table
% Returns: ok | {error, Reason}
%-------------------------------------------------------------

add(SubId, NagId, CustName, DevType, 
    TotalBadGbDown, TotalGbDown,
    TotalBadPeriods, TotalPeriodsActive, 
    TotalBadDays, TotalDaysActive, 
    City, Province, PostalCode) ->

    BadPeriodPctg = if TotalPeriodsActive /= 0 -> TotalBadPeriods / TotalPeriodsActive; true -> 0 end,
    BadPeriodPctgBucket = round(BadPeriodPctg * 10) / 10,
    
    Record = #subscribers{subscriber_id = SubId, 
                          nag_id = NagId, 
                          customer_name = CustName, 
                          device_type = DevType, 
                          total_bad_gb_down = TotalBadGbDown,
                          total_gb_down = TotalGbDown,
                          total_bad_periods = TotalBadPeriods, 
                          total_periods = TotalPeriodsActive, 
                          total_bad_days = TotalBadDays, 
                          total_days_active = TotalDaysActive, 
                          bad_period_pctg = BadPeriodPctg, 
                          bad_period_pctg_bucket = BadPeriodPctgBucket,
                          city = City, 
                          province = Province, 
                          postal_code = PostalCode},

    case mnesia:transaction(fun() -> mnesia:write(Record) end) of
        {atomic, ok} -> ok;
        {aborted, Reason} -> 
            io:format("record add aborted, ~w~n", [Reason]),
            {error, Reason}
    end.


%-------------------------------------------------------------
% Function: delete
% Purpose:  Deletes a record form the subscribers table
% Returns:  ok | {error, Reason}
%-------------------------------------------------------------

delete(SubId) -> 
    case mnesia:transaction(fun() -> mnesia:delete({subscribers, SubId}) end) of
        {atomic, ok} -> ok;
        {aborted, Reason} -> {error, Reason}
    end.

%-------------------------------------------------------------
% Function: read
% Purpose:  Reads a record from the subscribers table
% Returns:  {ok | Record} | {error, Reason}
%-------------------------------------------------------------

read(SubId) ->

    case mnesia:transaction(fun() -> mnesia:read({subscribers, SubId}) end) of
        {atomic, []} -> {error, not_found};
        {atomic, [Record]} -> {ok, Record};
        {aborted, Reason} -> {error, Reason}
    end.



%-------------------------------------------------------------
% Function: select
% Purpose:  Selects all the tuples that satisfy the specifications
% Returns:  {ok, List} | {error, Reason}
%-------------------------------------------------------------

select(FieldName, Operator, Value) -> 
    MatchHead = build_matchhead(),

    Fun = fun() -> 

        FieldPos = get_field_position(FieldName),

        % we increment the position by 1 since the matchhead
        % contains the table schema name, so everything has
        % shifted by one.
        Guard = [{Operator, element(FieldPos + 1, MatchHead), Value}],
        mnesia:select(subscribers, [{MatchHead, Guard, ['$_']}])
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
select_or(FieldName, Operator1, Value1, Operator2, Value2) -> 
    MatchHead = build_matchhead(),

    Fun = fun() -> 
        FieldPos = get_field_position(FieldName),

        % we increment the position by 1 since the matchhead
        % contains the table schema name, so everything has
        % shifted by one.
        %GuardTest = [{orelse, {'>', '$1', 3}, {'<', '$1', 5}}],
        
        Guard = [{'orelse', {Operator1, element(FieldPos + 1, MatchHead), Value1}, 
                          {Operator2, element(FieldPos + 1, MatchHead), Value2}}],
        mnesia:select(subscribers, [{MatchHead, Guard, ['$_']}])
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
select_and(FieldName, Operator1, Value1, Operator2, Value2) -> 
    MatchHead = build_matchhead(),

    Fun = fun() -> 
        FieldPos = get_field_position(FieldName),

        % we increment the position by 1 since the matchhead
        % contains the table schema name, so everything has
        % shifted by one.
        Guard = [{'andalso', {Operator1, element(FieldPos + 1, MatchHead), Value1}, 
                          {Operator2, element(FieldPos + 1, MatchHead), Value2}
                 }],
        mnesia:select(subscribers, [{MatchHead, Guard, ['$_']}])
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
get_field_position(FieldName) ->
    Fields = record_info(fields, subscribers),
    find_list_pos(FieldName, Fields).



%-------------------------------------------------------------
% Function: build_matchhead
% Purpose:  build the matchhead for the subscriber table to be 
%           used by select calls
% Returns:  Tuple
%-------------------------------------------------------------
%-spec build_matchhead() -> tuple()

build_matchhead() ->
    {subscribers, '$1', '$2', '$3', '$4', '$5', '$6', '$7', '$8', '$9', '$10', '$11', '$12', '$13', '$14', '$15'}.

                      