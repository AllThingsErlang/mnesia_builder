-module(db_access_ipc).
-include("../include/db_access_ipc.hrl").
-export([call/2,
         build_header/2, build_header/3,
         get_session_id/1,
         build_error/1, build_error/2,
         build_request_connect/0, build_response_connect/1,
         build_request_start_session/1, build_response_start_session/1, build_response_start_session/2,
         build_response_end_session/2,
         
         build_command_get_sessions/0,
         build_command_response_get_sessions/1,
         
         check_process_alive/1]).


%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
call(Pid, Message) -> 

    case db_access_ipc:check_process_alive(Pid) of 
        true -> gen_server:call(Pid, Message);
        false -> {error, {pid_not_alive, Pid}}
    end.

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
build_header(MessageType, MessageId) -> build_header({0, 0, 0}, MessageType, MessageId).

build_header(SessionId, MessageType, MessageId) when is_tuple(SessionId) -> 

    % Build it and then validate it. If valid, return it.
    Header = {{session_id, SessionId}, {MessageType, MessageId}},

    case MessageType of 
        ?MSG_TYPE_REQUEST -> 
            case MessageId of 
                ?REQUEST_CONNECT -> Header;
                ?REQUEST_START_SESSION -> Header
            end;

        ?MSG_TYPE_REQUEST_RESPONSE -> 
            case MessageId of 
                ?REQUEST_CONNECT -> Header;
                ?REQUEST_START_SESSION -> Header
            end;

        ?MSG_TYPE_COMMAND ->
            case MessageId of
                ?COMMAND_GET_SESSIONS -> Header
            end;

        ?MSG_TYPE_COMMAND_RESPONSE ->
            case MessageId of
                ?COMMAND_GET_SESSIONS -> Header
            end;

        error -> Header;
        _ -> {error, {invalid_message_type, MessageType}}
    end.

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
get_session_id(Message) -> 
    case Message of 
        {?PROT_VERSION, {Header, _}} ->
            case Header of 
                {{session_id, SessionId}, _} -> SessionId;
                _ -> {error, invalid_message} 
            end;

        _ -> {error, invalid_message} 
    end.

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
build_request_connect() ->

    Header = build_header(request, ?REQUEST_CONNECT),
    Payload = {},
    {?PROT_VERSION, {Header, Payload}}.


%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
build_response_connect(SessionId) ->

    Header = build_header(?MSG_TYPE_REQUEST_RESPONSE, ?REQUEST_CONNECT),
    {WorkerPid, _, _} = SessionId,
    Payload = {{result, ok}, {worker_pid, WorkerPid}, {session_id, SessionId}},

    {?PROT_VERSION, {Header, Payload}}.

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
build_request_start_session(SessionId) ->

    Header = build_header(SessionId, request, ?REQUEST_START_SESSION),
    Payload = {},

    {?PROT_VERSION, {Header, Payload}}.

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
build_response_start_session(SessionId) ->

    Header = build_header(SessionId, ?MSG_TYPE_REQUEST_RESPONSE, ?REQUEST_START_SESSION),
    Result = {result, ok},
    Payload = {Result},

    {?PROT_VERSION, {Header, Payload}}.

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
build_response_start_session(SessionId, Error) ->

    Header = build_header(SessionId, ?MSG_TYPE_REQUEST_RESPONSE, ?REQUEST_START_SESSION),
    Result = {result, {error, Error}},
    Payload = {Result},

    {?PROT_VERSION, {Header, Payload}}.

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
build_response_end_session(SessionId, Error) ->

    Header = build_header(SessionId, ?MSG_TYPE_REQUEST_RESPONSE, end_session),
    Result = {result, {?MSG_TYPE_ERROR, Error}},
    Payload = {Result},

    {?PROT_VERSION, {Header, Payload}}.

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
build_error(Error) -> build_error({0, 0, 0}, Error).

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
build_error(SessionId, Error) ->

    Header = build_header(SessionId, error, Error),
    Payload = {},
    {?PROT_VERSION, {Header, Payload}}.


%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
build_command_get_sessions() ->

    Header = build_header(?MSG_TYPE_COMMAND, ?COMMAND_GET_SESSIONS),
    Payload = {},

    {?PROT_VERSION, {Header, Payload}}.

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
build_command_response_get_sessions(SessionsList) ->

    Header = build_header(?MSG_TYPE_COMMAND_RESPONSE, ?COMMAND_GET_SESSIONS),
    Result = {result, ok},
    Payload = {Result, {sessions, SessionsList}},

    {?PROT_VERSION, {Header, Payload}}.



%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
check_process_alive(Pid) ->
    Nodes = [node() | nodes()],  % Include current node and all connected nodes
    lists:any(fun(Node) -> rpc:call(Node, erlang, is_process_alive, [Pid]) =:= true end, Nodes).
