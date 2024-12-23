-define(PROT_VERSION, "0.1").


%
% Protocol Structure
%
%   {Version, Message}
%
%      Message       =  {Header, Payload}}
%      Header        =  {{session_id, SessionID}, {MessageType, MessageName}}
%      SessionID     =  {WorkedPid, ClientPid, Token}
%      MessageType   =  request | request_response | error | command | command_response
%      MessageName   =  atom()
%
% Request message
%
%    {Version, { {{session_id, SessionId}, {request, MessageName}}, {...}}
%
% Request Response messages
%
%    {Version, { {{session_id, SessionId}, {request_response, MessageName}}, {{result, Result}, {...}}}}
%
%    Result = ok | {error, Reason}
%
% Error message
%
%   {Version, { {{session_id, SessionId}, {error, ErrorReason}}, {...}}
%
% Command message
%
%    {Version, { {{session_id, SessionId}, {command, MessageName}}, {...}}
%
% Command message
%
%    {Version, { {{session_id, SessionId}, {command_response, MessageName}}, {{result, Result}, ...}}
%

-define(SERVER_DB, db_server).
-define(SERVER_MODELLER, schema_modeller_server).

-define(MSG_SESSION_ID, session_id).

% Message Types
-define(MSG_TYPE_REQUEST, request).
-define(MSG_TYPE_REQUEST_RESPONSE, request_response).
-define(MSG_TYPE_ERROR, error).
-define(MSG_TYPE_COMMAND, command).
-define(MSG_TYPE_COMMAND_RESPONSE, command_respone).

% Request message IDs
% ---------------------------
% Session management
-define(REQUEST_CONNECT, connect).
-define(REQUEST_START_SESSION, start_session).
-define(REQUEST_END_SESSION, end_session).



% Command message IDs
% ---------------------------
-define(COMMAND_GET_SESSIONS, get_sessions).