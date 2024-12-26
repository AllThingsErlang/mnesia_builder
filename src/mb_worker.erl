-module(mb_worker).
-behaviour(gen_server).

-include("../include/mb_ipc.hrl").
-include("../include/mb_api.hrl").

-export([start/3, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(STATE_SERVER, server).
-define(STATE_SESSION_ID, session_id).
-define(STATE_SESSION_ACTIVE, session_active).
-define(STATE_CLIENT_PROCESS_REF, client_process_ref).
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
    io:format("[db::worker::~p]: serving client ~p with token ~p~n", [self(), ClientPid, Token]),
    process_flag(trap_exit, true),
    TimerRef = erlang:start_timer(30000, self(), session_start_timer),

    {ok, #{?STATE_SERVER=>ServerPid, 
           ?STATE_SESSION_ID=>{self(),ClientPid,Token}, 
           ?STATE_SESSION_ACTIVE=>false, 
           ?STATE_CLIENT_PROCESS_REF=>[],
           ?STATE_TIMER=>TimerRef, 
           ?STATE_SPECIFICATIONS=>mb_schemas:new()}}.


%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
handle_call({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, MessageId}}, Payload}}, From, State) ->

    ReceivedPid = mb_ipc:remove_pid_alias(From),
    io:format("[db::worker::~p]: received ~p from ~p~n", [self(), {?MSG_TYPE_REQUEST, MessageId}, ReceivedPid]),

    MySessionId = maps:get(?STATE_SESSION_ID, State),

    % Two levels of validation
    %    (1) Session ID 
    %    (2) Source (client PID)
    %    (3) Session is "active" if message is anything other than start_session, false if start_session

    case SessionId of 

        MySessionId -> 
            
            {_, ClientPid, _} = MySessionId,

            case ReceivedPid == ClientPid of

                true  -> 
                    
                    ExpectedMessage = (((MessageId == ?REQUEST_START_SESSION) and 
                                       not(maps:get(?STATE_SESSION_ACTIVE, State))) 
                                    or
                                      ((MessageId /= ?REQUEST_START_SESSION) and 
                                       maps:get(?STATE_SESSION_ACTIVE, State))),

                    case ExpectedMessage of 
                        true -> handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, MessageId}}, Payload}}, State);
                        false -> 
                            io:format("[db::worker::~p]: message rejected, unexpected_message ~n", [self()]),
                            ReplyMessage = mb_ipc:build_end_session_response(SessionId, {unexpected_message, {MessageId, {?STATE_SESSION_ACTIVE, maps:get(?STATE_SESSION_ACTIVE, State)}}}),
                            {reply, ReplyMessage, State}
                    end;

                false ->
                    io:format("[db::worker::~p]: message rejected, invalid_session_credentials, unrecognized_sender ~n", [self()]),
                    ReplyMessage = mb_ipc:build_end_session_response(SessionId, {invalid_session_credentials, unrecognized_sender}),
                    {reply, ReplyMessage, State}
            end;

        _ -> 
            io:format("[db::worker::~p]: message rejected, invalid_session_credentials, unrecognized_session_id ~n", [self()]),
            ReplyMessage = mb_ipc:build_end_session_response(SessionId, {invalid_session_credentials, unrecognized_session_id}),
            {reply, ReplyMessage, State}
    end;

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
handle_call(Request, _From, State) ->
    io:format("[db::worker::~p]: unsupported_request: ~p~n", [self(), Request]),

    ReplyMessage = mb_ipc:build_error(unsupported_request),
    {reply, ReplyMessage, State}.

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_START_SESSION}}, {}}}, State) ->

    io:format("[db::worker::~p]: ~p accepted, cancelling timer~n", [self(), ?REQUEST_START_SESSION]),

    TimerRef = maps:get(?STATE_TIMER, State),
    case TimerRef of 
        [] -> ok;
        _ -> erlang:cancel_timer(TimerRef)
    end,

    {_, ClientPid, _} = SessionId,
    ClientRef = erlang:monitor(process, ClientPid),

    UpdatedState = maps:update(?STATE_CLIENT_PROCESS_REF, ClientRef, 
                        maps:update(?STATE_TIMER, [], 
                            maps:update(?STATE_SESSION_ACTIVE, true, State))),

    ReplyMessage = mb_ipc:build_start_session_response(SessionId),
    {reply, ReplyMessage, UpdatedState};
                

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, _SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_END_SESSION}}, {}}}, State) ->

    io:format("[db::worker::~p]: ~p accepted~n", [self(), ?REQUEST_END_SESSION]),
    UpdatedState = maps:update(?STATE_SESSION_ACTIVE, false, State),
    {stop, normal, ok, UpdatedState};

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_NEW_SPECIFICATIONS}}, {}}}, State) ->

    UpdatedState = maps:update(?STATE_SPECIFICATIONS, mb_schemas:new(), State),
    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_NEW_SPECIFICATIONS, ok),
    {reply, ReplyMessage, UpdatedState};


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_GET_SPECIFICATIONS}}, {}}}, State) ->

    Result = {ok, maps:get(?STATE_SPECIFICATIONS, State)},
    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_GET_SPECIFICATIONS, Result),
    {reply, ReplyMessage, State};

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_ADD_SCHEMA}}, {{schema_name, SchemaName}}}}, State) ->

    SSG = maps:get(?STATE_SPECIFICATIONS, State),
    case mb_schemas:add_schema(SchemaName, SSG) of 
        {error, Reason} -> 
            UpdatedState = State,
            Result = {error, Reason};

        UpdatedSS ->
            UpdatedState = maps:update(?STATE_SPECIFICATIONS, UpdatedSS, State),
            Result = ok 
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_ADD_SCHEMA, Result),
    {reply, ReplyMessage, UpdatedState};


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_DELETE_SCHEMA}}, {{schema_name, SchemaName}}}}, State) ->

    SSG = maps:get(?STATE_SPECIFICATIONS, State),
    UpdatedSS = mb_schemas:delete_schema(SchemaName, SSG),
    UpdatedState = maps:update(?STATE_SPECIFICATIONS, UpdatedSS, State),
    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_DELETE_SCHEMA, ok),
    {reply, ReplyMessage, UpdatedState};

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_GET_SCHEMA}}, {{schema_name, SchemaName}}}}, State) ->

    SSG = maps:get(?STATE_SPECIFICATIONS, State),
    
    case mb_schemas:get_schema(SchemaName, SSG) of 
        {error, Reason} -> Result = {error, Reason};
        Schema -> Result = {ok, Schema}
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_GET_SCHEMA, Result),
    {reply, ReplyMessage, State};


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_SET_SCHEMA_ATTRIBUTES}}, {{schema_name, SchemaName}, {schema_avp_list, SchemaAvpList}}}}, State) ->

    SSG = maps:get(?STATE_SPECIFICATIONS, State),
    
    case mb_schemas:set_schema_attributes(SchemaAvpList, SchemaName, SSG) of 
        {error, Reason} -> 
            UpdatedState = State,
            Result = {error, Reason};

        UpdatedSS -> 
            UpdatedState = maps:update(?STATE_SPECIFICATIONS, UpdatedSS, State),
            Result = ok
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_SET_SCHEMA_ATTRIBUTES, Result),
    {reply, ReplyMessage, UpdatedState};

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_GET_SCHEMA_ATTRIBUTE}}, {{schema_name, SchemaName}, {schema_attribute, Attribute}}}}, State) ->

    SSG = maps:get(?STATE_SPECIFICATIONS, State),
    
    case mb_schemas:get_schema_attribute(Attribute, SchemaName, SSG) of 
        {error, Reason} -> Result = {error, Reason};
        Value -> Result = {ok, Value}
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_GET_SCHEMA_ATTRIBUTE, Result),
    {reply, ReplyMessage, State};


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_GET_SCHEMA_NAMES}}, {}}}, State) ->

    SSG = maps:get(?STATE_SPECIFICATIONS, State),
    
    Result = {ok, mb_schemas:schema_names(SSG)}, 
    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_GET_SCHEMA_NAMES, Result),
    {reply, ReplyMessage, State};


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_ADD_FIELD}}, {{schema_name, SchemaName}, {field_name, FieldName}}}}, State) ->

    SSG = maps:get(?STATE_SPECIFICATIONS, State),
    
    case mb_schemas:add_field(FieldName, SchemaName, SSG) of 
        {error, Reason} -> 
            UpdatedState = State,
            Result = {error, Reason};

        UpdatedSS -> 
            UpdatedState = maps:update(?STATE_SPECIFICATIONS, UpdatedSS, State),
            Result = ok 
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_ADD_FIELD, Result),
    {reply, ReplyMessage, UpdatedState};


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_SET_FIELD_ATTRIBUTES}}, {{schema_name, SchemaName}, {field_name, FieldName}, {field_avp_list, FieldAvpList}}}}, State) ->

    SSG = maps:get(?STATE_SPECIFICATIONS, State),
    
    case mb_schemas:set_field_attributes(FieldAvpList, FieldName, SchemaName, SSG) of 
        {error, Reason} -> 
            UpdatedState = State,
            Result = {error, Reason};

        UpdatedSS -> 
            UpdatedState = maps:update(?STATE_SPECIFICATIONS, UpdatedSS, State),
            Result = ok
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_SET_SCHEMA_ATTRIBUTES, Result),
    {reply, ReplyMessage, UpdatedState};

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_GET_FIELD_ATTRIBUTE}}, {{schema_name, SchemaName}, {field_name, FieldName}, {field_attribute, Attribute}}}}, State) ->

    SSG = maps:get(?STATE_SPECIFICATIONS, State),
    
    case mb_schemas:get_field_attribute(Attribute, FieldName, SchemaName, SSG) of 
        {error, Reason} -> Result = {error, Reason};
        Value -> Result = {ok, Value}
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_GET_FIELD_ATTRIBUTE, Result),
    {reply, ReplyMessage, State};

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_GET_FIELDS}}, {{schema_name, SchemaName}}}}, State) ->

    SSG = maps:get(?STATE_SPECIFICATIONS, State),
    
    case mb_schemas:fields(SchemaName, SSG) of 
        {error, Reason} -> Result = {error, Reason};
        Fields -> Result = {ok, Fields}
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_GET_FIELDS, Result),
    {reply, ReplyMessage, State};

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_GET_FIELD}}, {{schema_name, SchemaName}, {field_name, FieldName}}}}, State) ->

    SSG = maps:get(?STATE_SPECIFICATIONS, State),
    
    case mb_schemas:get_field(FieldName, SchemaName, SSG) of 
        {error, Reason} -> Result = {error, Reason};
        FieldSpec -> Result = {ok, FieldSpec}
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_GET_FIELD, Result),
    {reply, ReplyMessage, State};

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_GET_FIELD_COUNT}}, {{schema_name, SchemaName}}}}, State) ->

    SSG = maps:get(?STATE_SPECIFICATIONS, State),
    
    case mb_schemas:field_count(SchemaName, SSG) of 
        {error, Reason} -> Result = {error, Reason};
        Count -> Result = {ok, Count}
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_GET_FIELD_COUNT, Result),
    {reply, ReplyMessage, State};

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_GET_MANDATORY_FIELD_COUNT}}, {{schema_name, SchemaName}}}}, State) ->

    SSG = maps:get(?STATE_SPECIFICATIONS, State),
    
    case mb_schemas:mandatory_field_count(SchemaName, SSG) of 
        {error, Reason} -> Result = {error, Reason};
        Count -> Result = {ok, Count}
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_GET_MANDATORY_FIELD_COUNT, Result),
    {reply, ReplyMessage, State};


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_GET_FIELD_NAMES}}, {{schema_name, SchemaName}}}}, State) ->

    SSG = maps:get(?STATE_SPECIFICATIONS, State),
    
    case mb_schemas:field_names(SchemaName, SSG) of 
        {error, Reason} -> Result = {error, Reason};
        FieldNames -> Result = {ok, FieldNames}
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_GET_FIELD_NAMES, Result),
    {reply, ReplyMessage, State};

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_GET_FIELD_POSITION}}, {{schema_name, SchemaName}, {field_name, FieldName}}}}, State) ->

    SSG = maps:get(?STATE_SPECIFICATIONS, State),
    
    case mb_schemas:field_position(FieldName, SchemaName, SSG) of 
        {error, Reason} -> Result = {error, Reason};
        Position -> Result = {ok, Position}
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_GET_FIELD_POSITION, Result),
    {reply, ReplyMessage, State}.

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
handle_cast(Message, State) ->
    io:format("[db::worker::~p]: unsupported message: ~p~n", [self(), Message]),
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
                    io:format("[db::worker::~p]: unknown timer reference ~p~n", [self(), IncomingTimerRef]),
                    {noreply, State}
            end 
    end;

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
handle_info({'EXIT', _FromPid, Reason}, State) ->
    io:format("[db::worker::~p]: received exit signal, reason: ~p~n", [self(), Reason]),
    {stop, Reason, State};

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_info({'DOWN', _Ref, process, ClientPid, _Reason}, State) ->
    io:format("[db::worker::~p]: client disconnected~n", [self()]),

    % Handle client disconnection
    SessionId = maps:get(?STATE_SESSION_ID, State),

    case SessionId of
        {_, ClientPid, _} ->
            io:format("[db::worker::~p]:  client ~p disconnected. Stopping worker.~n", [self(), ClientPid]),
            {stop, normal, State};
        _ ->
            io:format("[db::worker::~p]:  no associated session id found: ~p.~n", [self(), SessionId]),
            {noreply, State}
    end;

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_info(Info, State) ->
    io:format("[db::worker::~p]: unsupported info notification: ~p~n", [self(), Info]),
    {noreply, State}.

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
terminate(_Reason, State) ->
    io:format("[db::worker::~p]: terminated for session ~p~n", [self(), maps:get(?STATE_SESSION_ID, State)]),
    ok.

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
