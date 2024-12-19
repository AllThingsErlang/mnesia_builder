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

-define(SERVER_DB, db_access_server).
-define(SERVER_MODELLER, schema_modeller_server).


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

% Specification management
-define(REQUEST_NEW_SPECIFICATIONS, new_specifications).
-define(REQUEST_GENERATE, generate).
-define(REQUEST_GET_SPECIFICATIONS, get_specifications).

% Schema management
-define(REQUEST_ADD_SCHEMA, add_schema).
-define(REQUEST_DELETE_SCHEMA, delete_schema).
-define(REQUEST_GET_SCHEMA, get_schema).
-define(REQUEST_SET_SCHEMA_ATTRIBUTES, set_schema_attributes).
-define(REQUEST_GET_SCHEMA_ATTRIBUTES, get_schema_attributes).
-define(REQUEST_GET_SCHEMA_NAMES, get_schema_names).

% Field management
-define(REQUEST_ADD_FIELD, add_field).
-define(REQUEST_SET_FIELD_ATTRIBUTES, set_field_attributes).
-define(REQUEST_GET_FIELD_ATTRIBUTES, get_field_attributes).
-define(REQUEST_GET_FIELDS, get_fields).
-define(REQUEST_GET_FIELD_COUNT, get_field_count).
-define(REQUEST_GET_MANDATORY_FIELD_COUNT, get_mandatory_field_count).
-define(REQUEST_GET_FIELD_NAMES, get_field_names).
-define(REQUEST_GET_FIELD_POSITION, get_field_position).

% Command message IDs
% ---------------------------
-define(COMMAND_GET_SESSIONS, get_sessions).