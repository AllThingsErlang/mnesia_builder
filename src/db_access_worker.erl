-module(db_access_worker).
-behaviour(gen_server).

-include("../include/db_access_ipc.hrl").
-include("../include/db_access_api.hrl").

-export([start/3, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(STATE_SERVER, server).
-define(STATE_SESSION_ID, session_id).
-define(STATE_SESSION_ACTIVE, session_active).
-define(STATE_TIMER, timer).
-define(STATE_SPECIFICATIONS, specifications).

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
start(ServerPid, ClientPid, Token) ->
    gen_server:start_link(?MODULE, {ServerPid, ClientPid, Token}, []).

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
init({ServerPid, ClientPid, Token}) ->
    io:format("[db_access::worker::~p]: serving client ~p with token ~p~n", [self(), ClientPid, Token]),
    process_flag(trap_exit, true),
    TimerRef = erlang:start_timer(30000, self(), session_start_timer),
    {ok, #{?STATE_SERVER=>ServerPid, ?STATE_SESSION_ID=>{self(),ClientPid,Token}, ?STATE_SESSION_ACTIVE=>false, ?STATE_TIMER=>TimerRef, ?STATE_SPECIFICATIONS=>schemas:new()}}.


%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
handle_call({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, MessageId}}, Payload}}, From, State) ->

    ReceivedPid = db_access_ipc:remove_pid_alias(From),
    io:format("[db_access::worker::~p]: received ~p from ~p~n", [self(), {?MSG_TYPE_REQUEST, MessageId}, ReceivedPid]),

    MySessionId = maps:get(?STATE_SESSION_ID, State),

    % Two levels of validation
    %    (1) Session ID 
    %    (2) Source (client PID)

    case SessionId of 

        MySessionId -> 
            
            {_, ClientPid, _} = MySessionId,

            case ReceivedPid == ClientPid of

                true  -> handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, MessageId}}, Payload}}, State);
                false ->
                    io:format("[db_access::worker::~p]: message rejected, invalid_session_credentials, unrecognized_sender ~n", [self()]),
                    ReplyMessage = db_access_ipc:build_end_session_response(SessionId, {invalid_session_credentials, unrecognized_sender}),
                    {reply, ReplyMessage, State}
            end;

        _ -> 
            io:format("[db_access::worker::~p]: message rejected, invalid_session_credentials, unrecognized_session_id ~n", [self()]),
            ReplyMessage = db_access_ipc:build_end_session_response(SessionId, {invalid_session_credentials, unrecognized_session_id}),
            {reply, ReplyMessage, State}
    end;

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
handle_call(Request, _From, State) ->
    io:format("[db_access::worker::~p]: unsupported_request: ~p~n", [self(), Request]),

    ReplyMessage = db_access_ipc:build_error(unsupported_request),
    {reply, ReplyMessage, State}.

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_START_SESSION}}, {}}}, State) ->

    io:format("[db_access::worker::~p]: ~p accepted, cancelling timer~n", [self(), ?REQUEST_START_SESSION]),

    TimerRef = maps:get(?STATE_TIMER, State),
    case TimerRef of 
        [] -> ok;
        _ -> erlang:cancel_timer(TimerRef)
    end,

    UpdatedState = maps:update(?STATE_TIMER, [], maps:update(?STATE_SESSION_ACTIVE, true, State)),
    ReplyMessage = db_access_ipc:build_start_session_response(SessionId),
    {reply, ReplyMessage, UpdatedState};
                

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, _SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_END_SESSION}}, {}}}, State) ->

    io:format("[db_access::worker::~p]: ~p accepted~n", [self(), ?REQUEST_END_SESSION]),
    UpdatedState = maps:update(?STATE_SESSION_ACTIVE, false, State),
    {stop, normal, ok, UpdatedState};

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_NEW_SPECIFICATIONS}}, {}}}, State) ->

    UpdatedState = maps:update(?STATE_SPECIFICATIONS, schemas:new(), State),
    ReplyMessage = db_access_ipc:build_request_response(SessionId, ?REQUEST_NEW_SPECIFICATIONS, ok),
    {reply, ReplyMessage, UpdatedState};


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_GET_SPECIFICATIONS}}, {}}}, State) ->

    ReplyMessage = db_access_ipc:build_request_response(SessionId, ?REQUEST_GET_SPECIFICATIONS, {ok, maps:get(?STATE_SPECIFICATIONS, State)}),
    {reply, ReplyMessage, State};

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_ADD_SCHEMA}}, {{schema_name, SchemaName}}}}, State) ->

    SS = maps:get(?STATE_SPECIFICATIONS, State),
    case schemas:add_schema(SchemaName, SS) of 
        {error, Reason} -> 
            UpdatedState = State,
            Result = {error, Reason};

        UpdatedSS ->
            UpdatedState = maps:update(?STATE_SPECIFICATIONS, UpdatedSS, State),
            Result = ok 
    end,

    ReplyMessage = db_access_ipc:build_request_response(SessionId, ?REQUEST_ADD_SCHEMA, Result),
    {reply, ReplyMessage, UpdatedState};


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_DELETE_SCHEMA}}, {{schema_name, SchemaName}}}}, State) ->

    SS = maps:get(?STATE_SPECIFICATIONS, State),
    UpdatedSS = schemas:delete_schema(SchemaName, SS),
    UpdatedState = maps:update(?STATE_SPECIFICATIONS, UpdatedSS, State),
    ReplyMessage = db_access_ipc:build_request_response(SessionId, ?REQUEST_DELETE_SCHEMA, ok),
    {reply, ReplyMessage, UpdatedState};

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_GET_SCHEMA}}, {{schema_name, SchemaName}}}}, State) ->

    SS = maps:get(?STATE_SPECIFICATIONS, State),
    
    case schemas:get_schema(SchemaName, SS) of 
        {error, Reason} -> Result = {error, Reason};
        Schema -> Result = {ok, Schema}
    end,

    ReplyMessage = db_access_ipc:build_request_response(SessionId, ?REQUEST_GET_SCHEMA, Result),
    {reply, ReplyMessage, State};


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_SET_SCHEMA_ATTRIBUTES}}, {{schema_name, SchemaName}, {schema_attributes, SchemaAvpList}}}}, State) ->

    SS = maps:get(?STATE_SPECIFICATIONS, State),
    
    case schemas:set_schema_attributes(SchemaAvpList, SchemaName, SS) of 
        {error, Reason} -> Result = {error, Reason};
        Schema -> Result = {ok, Schema}
    end,

    ReplyMessage = db_access_ipc:build_request_response(SessionId, ?REQUEST_GET_SCHEMA, Result),
    {reply, ReplyMessage, State}.



%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
handle_cast(Message, State) ->
    io:format("[db_access::worker::~p]: unsupported message: ~p~n", [self(), Message]),
    {noreply, State}.

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
handle_info({timeout, IncomingTimerRef, session_start_timer}, State) ->

    case maps:get(?STATE_SESSION_ACTIVE, State) of 
        true -> 
            % Ignore
            UpdatedState = maps:update(?STATE_TIMER, [], State),
            {noreply, UpdatedState};

        false -> 
            TimerRef = maps:get(?STATE_TIMER, State),

            case (IncomingTimerRef == TimerRef) of 
                true -> 
                    UpdatedState = maps:update(?STATE_SESSION_ACTIVE, false, State),
                    {stop, normal, UpdatedState};
                
                false -> 
                    io:format("[db_access::worker::~p]: unknown timer reference ~p~n", [self(), IncomingTimerRef]),
                    {noreply, State}
            end 
    end;

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
handle_info({'EXIT', _FromPid, Reason}, State) ->
    io:format("[db_access::worker::~p]: received exit signal, reason: ~p~n", [self(), Reason]),
    {stop, Reason, State};

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_info({'DOWN', _Ref, process, ClientPid, _Reason}, State) ->
    io:format("[db_access::worker::~p]: client disconnected~n", [self()]),

    % Handle client disconnection
    SessionId = maps:get(?STATE_SESSION_ID, State),

    case SessionId of
        {_, ClientPid, _} ->
            io:format("[db_access::worker::~p]:  client ~p disconnected. Stopping worker.~n", [self(), ClientPid]),
            {stop, normal, State};
        _ ->
            io:format("[db_access::worker::~p]:  no associated session id found: ~p.~n", [self(), SessionId]),
            {noreply, State}
    end;

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_info(Info, State) ->
    io:format("[db_access::worker::~p]: unsupported info notification: ~p~n", [self(), Info]),
    {noreply, State}.

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
terminate(_Reason, State) ->
    io:format("[db_access::worker::~p]: terminated for session ~p~n", [self(), maps:get(?STATE_SESSION_ID, State)]),
    ok.

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
