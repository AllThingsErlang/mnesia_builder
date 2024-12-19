-module(db_access_api).
-include("../include/db_access_ipc.hrl").

-export([connect/0,
         disconnect/1,
         get_sessions/0,
         
         new_specifications/1,
         add_schema/2,
         delete_schema/2,
         get_schema/2,
         set_schema_attributes/3,
         get_schema_attributes/3,
         get_schema_attributes/2,
         get_specifications/1,
         get_schema_names/1,
         generate/1,
         add_field/3,
         set_field_attributes/4,
         get_field_attributes/3,
         get_field_attributes/4,
         get_fields/2,
         get_field_count/2,
         get_mandatory_field_count/2,
         get_field_names/2,
         get_field_position/3]).



%-------------------------------------------------------------
% Session Management APIs
%-------------------------------------------------------------
connect() ->

    io:format("[db_access::api::~p]: connect ...~n", [self()]),

    ConnectMessage = db_access_ipc:build_connect_request(),
    ServerPid = global:whereis_name(?SERVER_DB),

    case ServerPid of 

        undefined -> 
            io:format("[db_access::api::~p]: server  ~p is not running~n", [self(), ?SERVER_DB]),
            {error, server_not_running};

        _ ->

            case db_access_ipc:call(ServerPid, ConnectMessage) of

                {?PROT_VERSION, {{{session_id, {0, 0, 0}}, {?MSG_TYPE_REQUEST_RESPONSE, ?REQUEST_CONNECT}}, {{result, ok}, {worker_pid, WorkerPid}, {session_id, NewSessionId}}}} ->

                    io:format("[db_access::api::~p]: session id ~p assigned, requesting session start from worker ~p~n", [self(), NewSessionId, WorkerPid]),

                    %timer:sleep(10000),
                    
                    StartMessage = db_access_ipc:build_start_session_request(NewSessionId),
                
                    case db_access_ipc:call(WorkerPid, StartMessage) of

                        {?PROT_VERSION, {{{session_id, NewSessionId}, {?MSG_TYPE_REQUEST_RESPONSE, ?REQUEST_START_SESSION}}, {{result, ok}}}} -> 
                            io:format("[db_access::api::~p]: session started~n", [self()]),
                            {ok, NewSessionId};

                        {?PROT_VERSION, {{{session_id, NewSessionId}, {?MSG_TYPE_REQUEST_RESPONSE, ?REQUEST_START_SESSION}}, {{result, {error, Reason}}}}} ->
                            io:format("[db_access::api::~p]: session start failed: ~p~n", [self(), Reason]),
                            {error, Reason};

                        Error -> 
                            io:format("[db_access::api::~p]: session start aborted: ~p~n", [self(), Error]),
                            {error, Error}
                    end;

                {?PROT_VERSION, {{{session_id, _SessionId}, {?MSG_TYPE_REQUEST_RESPONSE, ?REQUEST_CONNECT}}, {{result, {error, Reason}}}}} ->
                            io:format("[db_access::api::~p]: session connect failed: ~p~n", [self(), Reason]),
                            {error, Reason};

                Error ->
                    io:format("[db_access::api::~p]: connect aborted: ~p~n", [self(), Error]),
                    {error, Error}
            end
    end.


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
disconnect(SessionId) ->
    Message = db_access_ipc:build_request(SessionId, ?REQUEST_END_SESSION),
    db_access_ipc:worker_call(SessionId, Message).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
get_sessions() -> 

    io:format("[db_access::api::~p]: get_sessions ...~n", [self()]),

    GetSessionsMessage = db_access_ipc:build_command(?COMMAND_GET_SESSIONS),
    ServerPid = global:whereis_name(?SERVER_DB),

    case ServerPid of 

        undefined -> 
            io:format("[db_access::api::~p]: server  ~p is not running~n", [self(), ?SERVER_DB]),
            {error, server_not_running};

        _ ->

            case db_access_ipc:call(ServerPid, GetSessionsMessage) of

                {?PROT_VERSION, {{{session_id, {0, 0, 0}}, {?MSG_TYPE_COMMAND_RESPONSE, ?COMMAND_GET_SESSIONS}}, {{result, ok}, {sessions, SessionsList}}}} ->
                    io:format("[db_access::api::~p]: received sessions: ~n", [self()]),
                    {ok, SessionsList};

                {?PROT_VERSION, {{{session_id, {0, 0, 0}}, {?MSG_TYPE_COMMAND_RESPONSE, ?COMMAND_GET_SESSIONS}}, {{result, {error, Reason}}}}} ->
                    io:format("[db_access::api::~p]: get_sessions failed: ~p~n", [self(), Reason]);

                Error -> 
                        io:format("[db_access::api::~p]: get_sessions aborted: ~p~n", [self(), Error]),
                        {error, Error}
            end
    end.


                
%-------------------------------------------------------------
% Specificiations Management APIs
%-------------------------------------------------------------
new_specifications(SessionId) -> 
    Message = db_access_ipc:build_request(SessionId, ?REQUEST_NEW_SPECIFICATIONS),
    db_access_ipc:worker_call(SessionId, Message).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
get_specifications(SessionId) -> 
    Message = db_access_ipc:build_request(SessionId, ?REQUEST_GET_SPECIFICATIONS),
    db_access_ipc:worker_call(SessionId, Message).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
generate(SessionId) -> 
    Message = db_access_ipc:build_request(SessionId, ?REQUEST_GENERATE),
    db_access_ipc:worker_call(SessionId, Message).

%-------------------------------------------------------------
% Schema Management APIs
%-------------------------------------------------------------

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
add_schema(SessionId, SchemaName) -> 
    Message = db_access_ipc:build_request(SessionId, ?REQUEST_ADD_SCHEMA, {{schema_name, SchemaName}}),
    db_access_ipc:worker_call(SessionId, Message).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
delete_schema(SessionId, SchemaName) -> 
    Message = db_access_ipc:build_request(SessionId, ?REQUEST_DELETE_SCHEMA, {{schema_name, SchemaName}}),
    db_access_ipc:worker_call(SessionId, Message).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
get_schema(SessionId, SchemaName) ->  
    Message = db_access_ipc:build_request(SessionId, ?REQUEST_GET_SCHEMA, {{schema_name, SchemaName}}),
    db_access_ipc:worker_call(SessionId, Message).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
set_schema_attributes(SessionId, SchemaName, SchemaAvpList) -> 
    Message = db_access_ipc:build_request(SessionId, ?REQUEST_SET_SCHEMA_ATTRIBUTES, {{schema_name, SchemaName}, {schema_attributes, SchemaAvpList}}),
    db_access_ipc:worker_call(SessionId, Message).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
get_schema_attributes(SessionId, SchemaName) ->
    Message = db_access_ipc:build_request(SessionId, ?REQUEST_GET_SCHEMA_ATTRIBUTES, {{schema_name, SchemaName}}),
    db_access_ipc:worker_call(SessionId, Message).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
get_schema_attributes(SessionId, SchemaName, Attribute) ->
    Message = db_access_ipc:build_request(SessionId, ?REQUEST_GET_SCHEMA_ATTRIBUTES, {{schema_name, SchemaName}, {schema_attribute, Attribute}}),
    db_access_ipc:worker_call(SessionId, Message).


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
get_schema_names(SessionId) -> 
    Message = db_access_ipc:build_request(SessionId, ?REQUEST_GET_SCHEMA_NAMES),
    db_access_ipc:worker_call(SessionId, Message).

%-------------------------------------------------------------
% Field Management APIs
%-------------------------------------------------------------

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
add_field(SessionId, SchemaName, FieldName) -> 
    Message = db_access_ipc:build_request(SessionId, ?REQUEST_ADD_FIELD, {{schema_name, SchemaName}, {field_name, FieldName}}),
    db_access_ipc:worker_call(SessionId, Message).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
set_field_attributes(SessionId, SchemaName, FieldName, FieldAvpList) ->  
    Message = db_access_ipc:build_request(SessionId, ?REQUEST_SET_FIELD_ATTRIBUTES, {{schema_name, SchemaName}, {field_name, FieldName}, {field_attributes, FieldAvpList}}),
    db_access_ipc:worker_call(SessionId, Message).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
get_field_attributes(SessionId, SchemaName, FieldName) -> 
    Message = db_access_ipc:build_request(SessionId, ?REQUEST_GET_FIELD_ATTRIBUTES, {{schema_name, SchemaName}, {field_name, FieldName}}),
    db_access_ipc:worker_call(SessionId, Message).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
get_field_attributes(SessionId, SchemaName, FieldName, Attribute) -> 
    Message = db_access_ipc:build_request(SessionId, ?REQUEST_GET_FIELD_ATTRIBUTES, {{schema_name, SchemaName}, {field_name, FieldName}, {field_attribute, Attribute}}),
    db_access_ipc:worker_call(SessionId, Message).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
get_fields(SessionId, SchemaName) -> 
    Message = db_access_ipc:build_request(SessionId, ?REQUEST_GET_FIELDS, {{schema_name, SchemaName}}),
    db_access_ipc:worker_call(SessionId, Message).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
get_field_count(SessionId, SchemaName) -> 
    Message = db_access_ipc:build_request(SessionId, ?REQUEST_GET_FIELD_COUNT, {{schema_name, SchemaName}}),
    db_access_ipc:worker_call(SessionId, Message).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
get_mandatory_field_count(SessionId, SchemaName) ->
    Message = db_access_ipc:build_request(SessionId, ?REQUEST_GET_MANDATORY_FIELD_COUNT, {{schema_name, SchemaName}}),
    db_access_ipc:worker_call(SessionId, Message).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
get_field_names(SessionId, SchemaName) -> 
    Message = db_access_ipc:build_request(SessionId, ?REQUEST_GET_FIELD_NAMES, {{schema_name, SchemaName}}),
    db_access_ipc:worker_call(SessionId, Message).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
get_field_position(SessionId, SchemaName, FieldName) -> 
    Message = db_access_ipc:build_request(SessionId, ?REQUEST_GET_FIELD_POSITION, {{schema_name, SchemaName}, {field_name, FieldName}}),
    db_access_ipc:worker_call(SessionId, Message).





