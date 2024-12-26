-module(mb_api).
-include("../include/mb_ipc.hrl").
-include("../include/mb_api.hrl").
-include("../include/mb.hrl").

-export([connect/0,
         disconnect/1,
         get_sessions/0,
         
         new_specifications/1,
         new_specifications/5,
         add_schema/2,
         delete_schema/2,
         get_schema/2,
         set_schema_attributes/3,
         get_schema_attribute/3,
         get_specifications/1,
         get_schema_names/1,
         generate/1,
         add_field/3,
         set_field_attributes/4,
         get_field_attribute/4,
         get_fields/2,
         get_field/3,
         get_field_count/2,
         get_mandatory_field_count/2,
         get_field_names/2,
         get_field_position/3]).


%-------------------------------------------------------------
% IPC specific types
%-------------------------------------------------------------
-type mb_session_id() :: {pid(), pid(), integer()}.
%-------------------------------------------------------------


%-------------------------------------------------------------
% Session Management APIs
% Connects to the server within the cluster and returns a SessionId.
%-------------------------------------------------------------
-spec connect() -> {ok, mb_session_id()} | mb_error().
%-------------------------------------------------------------
connect() ->

    io:format("[db::api::~p]: connect ...~n", [self()]),

    ConnectMessage = mb_ipc:build_connect_request(),
    ServerPid = global:whereis_name(?SERVER_DB),

    case ServerPid of 

        undefined -> 
            io:format("[db::api::~p]: server  ~p is not running~n", [self(), ?SERVER_DB]),
            {error, server_not_running};

        _ ->

            case mb_ipc:call(ServerPid, ConnectMessage) of

                {?PROT_VERSION, {{{session_id, {0, 0, 0}}, {?MSG_TYPE_REQUEST_RESPONSE, ?REQUEST_CONNECT}}, {{result, ok}, {worker_pid, WorkerPid}, {session_id, NewSessionId}}}} ->

                    io:format("[db::api::~p]: session id ~p assigned, requesting session start from worker ~p~n", [self(), NewSessionId, WorkerPid]),

                    %timer:sleep(10000),
                    
                    StartMessage = mb_ipc:build_start_session_request(NewSessionId),
                
                    case mb_ipc:call(WorkerPid, StartMessage) of

                        {?PROT_VERSION, {{{session_id, NewSessionId}, {?MSG_TYPE_REQUEST_RESPONSE, ?REQUEST_START_SESSION}}, {{result, ok}}}} -> 
                            io:format("[db::api::~p]: session started~n", [self()]),
                            {ok, NewSessionId};

                        {?PROT_VERSION, {{{session_id, NewSessionId}, {?MSG_TYPE_REQUEST_RESPONSE, ?REQUEST_START_SESSION}}, {{result, {error, Reason}}}}} ->
                            io:format("[db::api::~p]: session start failed: ~p~n", [self(), Reason]),
                            {error, Reason};

                        Error -> 
                            io:format("[db::api::~p]: session start aborted: ~p~n", [self(), Error]),
                            {error, Error}
                    end;

                {?PROT_VERSION, {{{session_id, _SessionId}, {?MSG_TYPE_REQUEST_RESPONSE, ?REQUEST_CONNECT}}, {{result, {error, Reason}}}}} ->
                            io:format("[db::api::~p]: session connect failed: ~p~n", [self(), Reason]),
                            {error, Reason};

                Error ->
                    io:format("[db::api::~p]: connect aborted: ~p~n", [self(), Error]),
                    {error, Error}
            end
    end.


%-------------------------------------------------------------
% Disconnects from the server and terminates the session.
%-------------------------------------------------------------
-spec disconnect(tuple()) -> {ok, term()} | mb_error().
%-------------------------------------------------------------
disconnect(SessionId) ->
    Message = mb_ipc:build_request(SessionId, ?REQUEST_END_SESSION),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec get_sessions() -> {ok, []} | {ok, [mb_session_id()]} | mb_error().
%-------------------------------------------------------------
get_sessions() -> 

    io:format("[db::api::~p]: get_sessions ...~n", [self()]),

    GetSessionsMessage = mb_ipc:build_command(?COMMAND_GET_SESSIONS),
    ServerPid = global:whereis_name(?SERVER_DB),

    case ServerPid of 

        undefined -> 
            io:format("[db::api::~p]: server  ~p is not running~n", [self(), ?SERVER_DB]),
            {error, server_not_running};

        _ ->

            case mb_ipc:call(ServerPid, GetSessionsMessage) of

                {?PROT_VERSION, {{{session_id, {0, 0, 0}}, {?MSG_TYPE_COMMAND_RESPONSE, ?COMMAND_GET_SESSIONS}}, {result, {ok, SessionsList}}}} ->
                    io:format("[db::api::~p]: received sessions: ~n", [self()]),
                    {ok, SessionsList};

                {?PROT_VERSION, {{{session_id, {0, 0, 0}}, {?MSG_TYPE_COMMAND_RESPONSE, ?COMMAND_GET_SESSIONS}}, {{result, {error, Reason}}}}} ->
                    io:format("[db::api::~p]: get_sessions failed: ~p~n", [self(), Reason]);

                Error -> 
                        io:format("[db::api::~p]: get_sessions aborted: ~p~n", [self(), Error]),
                        {error, Error}
            end
    end.
    
%-------------------------------------------------------------
% Specificiations Management APIs
%-------------------------------------------------------------


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec new_specifications(mb_session_id()) -> ok | mb_error().
%-------------------------------------------------------------
new_specifications(SessionId) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_NEW_SPECIFICATIONS),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec new_specifications(mb_session_id(), mb_ssg_name(), string(), string(), string()) -> ok | mb_error().
%-------------------------------------------------------------
new_specifications(SessionId, Name, Owner, Email, Description) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_NEW_SPECIFICATIONS, {{name, Name}, {owner, Owner}, {email, Email}, {description, Description}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec get_specifications(mb_session_id()) -> {ok, mb_ssg()} | mb_error().
%-------------------------------------------------------------
get_specifications(SessionId) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_GET_SPECIFICATIONS),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
generate(SessionId) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_GENERATE),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).

%-------------------------------------------------------------
% Schema Management APIs
%-------------------------------------------------------------

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
add_schema(SessionId, SchemaName) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_ADD_SCHEMA, {{schema_name, SchemaName}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
delete_schema(SessionId, SchemaName) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_DELETE_SCHEMA, {{schema_name, SchemaName}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
get_schema(SessionId, SchemaName) ->  
    Message = mb_ipc:build_request(SessionId, ?REQUEST_GET_SCHEMA, {{schema_name, SchemaName}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
set_schema_attributes(SessionId, SchemaName, SchemaAvpList) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_SET_SCHEMA_ATTRIBUTES, {{schema_name, SchemaName}, {schema_avp_list, SchemaAvpList}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
get_schema_attribute(SessionId, SchemaName, Attribute) ->
    Message = mb_ipc:build_request(SessionId, ?REQUEST_GET_SCHEMA_ATTRIBUTE, {{schema_name, SchemaName}, {schema_attribute, Attribute}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
get_schema_names(SessionId) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_GET_SCHEMA_NAMES),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).

%-------------------------------------------------------------
% Field Management APIs
%-------------------------------------------------------------

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
add_field(SessionId, SchemaName, FieldName) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_ADD_FIELD, {{schema_name, SchemaName}, {field_name, FieldName}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
set_field_attributes(SessionId, SchemaName, FieldName, FieldAvpList) ->  
    Message = mb_ipc:build_request(SessionId, ?REQUEST_SET_FIELD_ATTRIBUTES, {{schema_name, SchemaName}, {field_name, FieldName}, {field_avp_list, FieldAvpList}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
get_field_attribute(SessionId, SchemaName, FieldName, Attribute) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_GET_FIELD_ATTRIBUTE, {{schema_name, SchemaName}, {field_name, FieldName}, {field_attribute, Attribute}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
get_fields(SessionId, SchemaName) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_GET_FIELDS, {{schema_name, SchemaName}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
get_field(SessionId, SchemaName, FieldName) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_GET_FIELD, {{schema_name, SchemaName}, {field_name, FieldName}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
get_field_count(SessionId, SchemaName) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_GET_FIELD_COUNT, {{schema_name, SchemaName}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
get_mandatory_field_count(SessionId, SchemaName) ->
    Message = mb_ipc:build_request(SessionId, ?REQUEST_GET_MANDATORY_FIELD_COUNT, {{schema_name, SchemaName}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
get_field_names(SessionId, SchemaName) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_GET_FIELD_NAMES, {{schema_name, SchemaName}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
get_field_position(SessionId, SchemaName, FieldName) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_GET_FIELD_POSITION, {{schema_name, SchemaName}, {field_name, FieldName}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).



%-------------------------------------------------------------
% 
%-------------------------------------------------------------
request_response_result({?PROT_VERSION, {{_SessionId, {?MSG_TYPE_REQUEST_RESPONSE, _}}, {result, Result}}}) -> Result;
request_response_result(Other) -> {error, {unrecognized_reply, Other}}.
