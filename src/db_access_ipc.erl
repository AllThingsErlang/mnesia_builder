-module(db_access_ipc).
-include("../include/db_access_ipc.hrl").
-export([call/2,
         worker_call/2,
         build_header/2, build_header/3,
         get_session_id/1,
         build_error/1, build_error/2,
         build_connect_request/0, build_connect_response/1,
         build_start_session_request/1, build_start_session_response/1, build_start_session_response/2,
         build_end_session_response/2,
         
         build_command/1,
         build_command_response/2,
         
         build_request/2,
         build_request/3,
         build_request_response/3,
         
         remove_pid_alias/1,
         check_process_alive/1]).


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
call(Pid, Message) -> 

    case db_access_ipc:check_process_alive(Pid) of 
        true -> gen_server:call(Pid, Message);
        false -> {error, {pid_not_alive, Pid}}
    end.

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
worker_call(SessionId, Message) when is_tuple(Message) -> 

    {WorkerPid, _ClientPid, _Token} = SessionId,
    call(WorkerPid, Message).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
build_header(MessageType, MessageId) -> build_header({0, 0, 0}, MessageType, MessageId).
build_header(SessionId, MessageType, MessageId) when is_tuple(SessionId) -> {{session_id, SessionId}, {MessageType, MessageId}}.

%-------------------------------------------------------------
% 
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
% 
%-------------------------------------------------------------
build_connect_request() ->

    Header = build_header(request, ?REQUEST_CONNECT),
    Payload = {},
    {?PROT_VERSION, {Header, Payload}}.


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
build_connect_response(SessionId) ->

    Header = build_header(?MSG_TYPE_REQUEST_RESPONSE, ?REQUEST_CONNECT),
    {WorkerPid, _, _} = SessionId,
    Payload = {{result, ok}, {worker_pid, WorkerPid}, {session_id, SessionId}},

    {?PROT_VERSION, {Header, Payload}}.

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
build_start_session_request(SessionId) ->

    Header = build_header(SessionId, ?MSG_TYPE_REQUEST, ?REQUEST_START_SESSION),
    Payload = {},

    {?PROT_VERSION, {Header, Payload}}.

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
build_start_session_response(SessionId) ->

    Header = build_header(SessionId, ?MSG_TYPE_REQUEST_RESPONSE, ?REQUEST_START_SESSION),
    Result = {result, ok},
    Payload = {Result},

    {?PROT_VERSION, {Header, Payload}}.

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
build_start_session_response(SessionId, Error) ->

    Header = build_header(SessionId, ?MSG_TYPE_REQUEST_RESPONSE, ?REQUEST_START_SESSION),
    Result = {result, {error, Error}},
    Payload = {Result},

    {?PROT_VERSION, {Header, Payload}}.

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
build_end_session_response(SessionId, Error) ->

    Header = build_header(SessionId, ?MSG_TYPE_REQUEST_RESPONSE, end_session),
    Result = {result, {?MSG_TYPE_ERROR, Error}},
    Payload = {Result},

    {?PROT_VERSION, {Header, Payload}}.


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
build_request(SessionId, RequestId) -> build_request(SessionId, RequestId, {}).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
build_request(SessionId, RequestId, Payload) ->

    Header = build_header(SessionId, ?MSG_TYPE_REQUEST, RequestId),
    {?PROT_VERSION, {Header, Payload}}.


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
build_request_response(SessionId, RequestId, Result) ->

    Header = build_header(SessionId, ?MSG_TYPE_REQUEST_RESPONSE, RequestId),
    Payload = {result, Result},
    {?PROT_VERSION, {Header, Payload}}.




%-------------------------------------------------------------
% 
%-------------------------------------------------------------
build_command(CommandId) ->

    Header = build_header(?MSG_TYPE_COMMAND, CommandId),
    Payload = {},

    {?PROT_VERSION, {Header, Payload}}.

%-------------------------------------------------------------
% 
%--------------------------------------------------------------
build_command_response(CommandId, SessionsList) ->

    Header = build_header(?MSG_TYPE_COMMAND_RESPONSE, CommandId),
    Result = {result, ok},
    Payload = {Result, {sessions, SessionsList}},

    {?PROT_VERSION, {Header, Payload}}.




%-------------------------------------------------------------
% 
%-------------------------------------------------------------
build_error(Error) -> build_error({0, 0, 0}, Error).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
build_error(SessionId, Error) ->

    Header = build_header(SessionId, error, Error),
    Payload = {},
    {?PROT_VERSION, {Header, Payload}}.

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
remove_pid_alias({Pid, [alias|_]}) -> Pid;
remove_pid_alias(Pid) -> Pid.



%-------------------------------------------------------------
% 
%-------------------------------------------------------------
check_process_alive(Pid) ->
    Nodes = [node() | nodes()],  % Include current node and all connected nodes
    lists:any(fun(Node) -> rpc:call(Node, erlang, is_process_alive, [Pid]) =:= true end, Nodes).