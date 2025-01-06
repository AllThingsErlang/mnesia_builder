-module(mb_api).
-include("../include/mb_ipc.hrl").
-include("../include/mb_api.hrl").
-include("../include/mb.hrl").

-export([connect/0,
         disconnect/1,
         get_sessions/0,
         new_ssg/1,
         new_ssg/5,
         set_ssg_name/2,
         set_ssg_owner/2,
         set_ssg_email/2,
         set_ssg_description/2,
         use_module/1,
         use_ssg/1,
         upload_module/2,
         upload_module/3,
         upload_module/4,
         download_module/1,
         download_module/2,
         validate_ssg/1,
         generate/1,
         install/1,

         add_schema/2,
         delete_schema/2,
         get_schema/2,
         get_all_schemas/1,

         set_schema_name/3,
         set_schema_type/3,
         set_schema_ram_copies_local/2,
         set_schema_ram_copies/3,

         set_schema_disc_copies_local/2,
         set_schema_disc_copies/3,

         set_schema_disc_only_copies_local/2,
         set_schema_disc_only_copies/3,

         get_schema_type/2,
         get_schema_ram_copies/2,
         get_schema_disc_copies/2,
         get_schema_disc_only_copies/2,

         get_schema_attribute/3,
         get_ssg/1,
         get_schema_names/1,
         
         add_field/3,
         make_field_key/3,
         move_field/4,

         set_field_type/4,
         set_field_label/4,
         set_field_priority/4,
         set_field_default_value/4,
         set_field_description/4,

         get_field_type/3,
         get_field_label/3,
         get_field_priority/3,
         get_field_default_value/3,
         get_field_description/3,

         get_field_attribute/4,
         get_fields/2,
         get_field/3,
         get_field_count/2,
         get_mandatory_field_count/2,
         get_field_names/2,
         get_field_position/3,
        
         read/3,
         select/5,
         select_or/7,
         select_and/7,
        
         add_record/2,
         add_record/3,
         add_record/4]).


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
-spec new_ssg(mb_session_id()) -> ok | mb_error().
%-------------------------------------------------------------
new_ssg(SessionId) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_NEW_SSG),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec new_ssg(mb_session_id(), mb_ssg_name(), string(), string(), string()) -> ok | mb_error().
%-------------------------------------------------------------
new_ssg(SessionId, Name, Owner, Email, Description) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_NEW_SSG, {{name, Name}, {owner, Owner}, {email, Email}, {description, Description}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec get_ssg(mb_session_id()) -> {ok, mb_ssg()} | mb_error().
%-------------------------------------------------------------
get_ssg(SessionId) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_GET_SSG),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec set_ssg_name(mb_session_id(), atom()) -> ok | mb_error().
%-------------------------------------------------------------
set_ssg_name(SessionId, Name) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_SET_SSG_NAME, {{name, Name}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec set_ssg_owner(mb_session_id(), string()) -> ok | mb_error().
%-------------------------------------------------------------
set_ssg_owner(SessionId, Owner) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_SET_SSG_OWNER, {{owner, Owner}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec set_ssg_email(mb_session_id(), string()) -> ok | mb_error().
%-------------------------------------------------------------
set_ssg_email(SessionId, Email) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_SET_SSG_EMAIL, {{email, Email}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec set_ssg_description(mb_session_id(), string()) -> ok | mb_error().
%-------------------------------------------------------------
set_ssg_description(SessionId, Description) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_SET_SSG_DESCRIPTION, {{description, Description}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec use_module(mb_session_id()) -> ok | mb_error().
%-------------------------------------------------------------
use_module(SessionId) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_USE_MODULE, {{use_module, true}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec use_ssg(mb_session_id()) -> ok | mb_error().
%-------------------------------------------------------------
use_ssg(SessionId) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_USE_MODULE, {{use_module, false}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec upload_module(mb_session_id(), atom()) -> ok | mb_error().
%-------------------------------------------------------------
upload_module(SessionId, Module) -> upload_module(SessionId, ".", Module, false).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec upload_module(mb_session_id(), atom(), boolean()) -> ok | mb_error().
%-------------------------------------------------------------
upload_module(SessionId, Module, ForceLoadFlag) -> upload_module(SessionId, ".", Module, ForceLoadFlag).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec upload_module(mb_session_id(), string(), atom(), boolean()) -> ok | mb_error().
%-------------------------------------------------------------
upload_module(SessionId, Path, Module, ForceLoadFlag) when is_list(Path), is_atom(Module), is_boolean(ForceLoadFlag) -> 

    case file:read_file(Path ++ "/" ++ atom_to_list(Module) ++ ".erl") of
        {ok, Binary} ->
            Message = mb_ipc:build_request(SessionId, ?REQUEST_UPLOAD_MODULE, {{module_name, Module}, {module, Binary}, {force_load, ForceLoadFlag}}),
            Reply = mb_ipc:worker_call(SessionId, Message),
            request_response_result(Reply);

        {error, Reason} -> {error, Reason}
    end;

upload_module(_SessionId, Path, Module, _) -> {error, {invalid_argument, {Path, Module}}}.

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec download_module(mb_session_id()) -> ok | mb_error().
%-------------------------------------------------------------
download_module(SessionId) -> download_module(SessionId, ".").


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec download_module(mb_session_id(), string()) -> ok | mb_error().
%-------------------------------------------------------------
download_module(SessionId, Path) when is_list(Path) -> 

    Message = mb_ipc:build_request(SessionId, ?REQUEST_DOWNLOAD_MODULE, {}),
    Reply = mb_ipc:worker_call(SessionId, Message),

    case request_response_result(Reply) of 
        {ok, {Module, SrcBinary, HrlBinary}} -> 
            
            case file:write_file(Path ++ "/" ++ atom_to_list(Module) ++ ".erl", SrcBinary) of 
                ok -> file:write_file(Path ++ "/" ++ atom_to_list(Module) ++ ".hrl", HrlBinary);
                {error, Reason} -> {error, Reason}
            end;

        {error, Reason} -> {error, Reason}
    end.

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec validate_ssg(mb_session_id()) -> ok | mb_error().
%-------------------------------------------------------------
validate_ssg(SessionId) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_VALIDATE_SSG),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec generate(mb_session_id()) -> ok | mb_error().
%-------------------------------------------------------------
generate(SessionId) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_GENERATE),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec install(mb_session_id()) -> ok | mb_error().
%-------------------------------------------------------------
install(SessionId) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_INSTALL),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).

%-------------------------------------------------------------
% Schema Management APIs
%-------------------------------------------------------------

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec add_schema(mb_session_id(), mb_schema_name()) -> ok | mb_error().
%-------------------------------------------------------------
add_schema(SessionId, SchemaName) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_ADD_SCHEMA, {{schema_name, SchemaName}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec delete_schema(mb_session_id(), mb_schema_name()) -> ok | mb_error().
%-------------------------------------------------------------
delete_schema(SessionId, SchemaName) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_DELETE_SCHEMA, {{schema_name, SchemaName}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec get_schema(mb_session_id(), mb_schema_name()) -> mb_schema_spec() | mb_error().
%-------------------------------------------------------------
get_schema(SessionId, SchemaName) ->  
    Message = mb_ipc:build_request(SessionId, ?REQUEST_GET_SCHEMA, {{schema_name, SchemaName}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec get_all_schemas(mb_session_id()) -> mb_schema_spec_list() | mb_error().
%-------------------------------------------------------------
get_all_schemas(SessionId) ->  
    Message = mb_ipc:build_request(SessionId, ?REQUEST_GET_ALL_SCHEMAS),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec set_schema_name(mb_session_id(), mb_schema_name(), mb_schema_name()) -> ok | mb_error().
%-------------------------------------------------------------
set_schema_name(SessionId, SchemaOldName, SchemaNewName) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_SET_SCHEMA_NAME, {{schema_old_name, SchemaOldName}, {schema_new_name, SchemaNewName}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).
 
%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec set_schema_type(mb_session_id(), mb_schema_name(), mb_schema_type()) -> ok | mb_error().
%-------------------------------------------------------------
set_schema_type(SessionId, SchemaName, SchemaType) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_SET_SCHEMA_TYPE, {{schema_name, SchemaName}, {?SCHEMA_TYPE, SchemaType}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec set_schema_ram_copies_local(mb_session_id(), mb_schema_name()) -> ok | mb_error().
%-------------------------------------------------------------
set_schema_ram_copies_local(SessionId, SchemaName) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_SET_SCHEMA_RAM_COPIES, {{schema_name, SchemaName}, {?RAM_COPIES, [?KEYWORD_LOCAL_NODE]}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec set_schema_ram_copies(mb_session_id(), mb_schema_name(), list()) -> ok | mb_error().
%-------------------------------------------------------------
set_schema_ram_copies(SessionId, SchemaName, NodesList) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_SET_SCHEMA_RAM_COPIES, {{schema_name, SchemaName}, {?RAM_COPIES, NodesList}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec set_schema_disc_copies_local(mb_session_id(), mb_schema_name()) -> ok | mb_error().
%-------------------------------------------------------------
set_schema_disc_copies_local(SessionId, SchemaName) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_SET_SCHEMA_DISC_COPIES, {{schema_name, SchemaName}, {?DISC_COPIES, [?KEYWORD_LOCAL_NODE]}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec set_schema_disc_copies(mb_session_id(), mb_schema_name(), list()) -> ok | mb_error().
%-------------------------------------------------------------
set_schema_disc_copies(SessionId, SchemaName, NodesList) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_SET_SCHEMA_DISC_COPIES, {{schema_name, SchemaName}, {?DISC_COPIES, NodesList}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec set_schema_disc_only_copies_local(mb_session_id(), mb_schema_name()) -> ok | mb_error().
%-------------------------------------------------------------
set_schema_disc_only_copies_local(SessionId, SchemaName) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_SET_SCHEMA_DISC_ONLY_COPIES, {{schema_name, SchemaName}, {?DISC_ONLY_COPIES, [?KEYWORD_LOCAL_NODE]}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec set_schema_disc_only_copies(mb_session_id(), mb_schema_name(), list()) -> ok | mb_error().
%-------------------------------------------------------------
set_schema_disc_only_copies(SessionId, SchemaName, NodesList) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_SET_SCHEMA_DISC_ONLY_COPIES, {{schema_name, SchemaName}, {?DISC_ONLY_COPIES, NodesList}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).

 

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec get_schema_type(mb_session_id(), mb_schema_name()) -> ok | mb_error().
%-------------------------------------------------------------
get_schema_type(SessionId, SchemaName) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_GET_SCHEMA_TYPE, {{schema_name, SchemaName}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec get_schema_ram_copies(mb_session_id(), mb_schema_name()) -> ok | mb_error().
%-------------------------------------------------------------
get_schema_ram_copies(SessionId, SchemaName) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_GET_SCHEMA_RAM_COPIES, {{schema_name, SchemaName}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec get_schema_disc_copies(mb_session_id(), mb_schema_name()) -> ok | mb_error().
%-------------------------------------------------------------
get_schema_disc_copies(SessionId, SchemaName) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_GET_SCHEMA_DISC_COPIES, {{schema_name, SchemaName}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec get_schema_disc_only_copies(mb_session_id(), mb_schema_name()) -> ok | mb_error().
%-------------------------------------------------------------
get_schema_disc_only_copies(SessionId, SchemaName) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_GET_SCHEMA_DISC_ONLY_COPIES, {{schema_name, SchemaName}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).



%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec get_schema_attribute(mb_session_id(), mb_schema_name(), mb_schema_attribute()) -> term() | mb_error().
%-------------------------------------------------------------
get_schema_attribute(SessionId, SchemaName, Attribute) ->
    Message = mb_ipc:build_request(SessionId, ?REQUEST_GET_SCHEMA_ATTRIBUTE, {{schema_name, SchemaName}, {schema_attribute, Attribute}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec get_schema_names(mb_session_id()) -> [] | [mb_schema_name()] | mb_error().
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
make_field_key(SessionId, SchemaName, FieldName) ->  
    Message = mb_ipc:build_request(SessionId, ?REQUEST_MAKE_FIELD_KEY, {{schema_name, SchemaName}, {field_name, FieldName}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
move_field(SessionId, SchemaName, FieldName, ToPosition) ->  
    Message = mb_ipc:build_request(SessionId, ?REQUEST_MOVE_FIELD, {{schema_name, SchemaName}, {field_name, FieldName}, {position, ToPosition}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec set_field_type(mb_session_id(), mb_schema_name(), mb_field_name(), term()) -> ok | mb_error().
%-------------------------------------------------------------
set_field_type(SessionId, SchemaName, FieldName, NewType) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_SET_FIELD_TYPE, {{schema_name, SchemaName}, {field_name, FieldName}, {?FIELD_TYPE, NewType}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).

%-------------------------------------------------------------
%
%-------------------------------------------------------------
-spec set_field_description(mb_session_id(), mb_schema_name(), mb_field_name(), term()) -> ok | mb_error().
%-------------------------------------------------------------
set_field_description(SessionId, SchemaName, FieldName, NewDescription) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_SET_FIELD_DESCRIPTION, {{schema_name, SchemaName}, {field_name, FieldName}, {?DESCRIPTION, NewDescription}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).    


%-------------------------------------------------------------
%
%-------------------------------------------------------------
-spec set_field_label(mb_session_id(), mb_schema_name(), mb_field_name(), term()) -> ok | mb_error().
%-------------------------------------------------------------
set_field_label(SessionId, SchemaName, FieldName, NewLabel) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_SET_FIELD_LABEL, {{schema_name, SchemaName}, {field_name, FieldName}, {?LABEL, NewLabel}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).  

%-------------------------------------------------------------
%
%-------------------------------------------------------------
-spec set_field_priority(mb_session_id(), mb_schema_name(), mb_field_name(), term()) -> ok | mb_error().
%-------------------------------------------------------------
set_field_priority(SessionId, SchemaName, FieldName, NewPriority) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_SET_FIELD_PRIORITY, {{schema_name, SchemaName}, {field_name, FieldName}, {?PRIORITY, NewPriority}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply). 

%-------------------------------------------------------------
%
%-------------------------------------------------------------
-spec set_field_default_value(mb_session_id(), mb_schema_name(), mb_field_name(), term()) -> ok | mb_error().
%-------------------------------------------------------------
set_field_default_value(SessionId, SchemaName, FieldName, NewDefaultValue) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_SET_FIELD_DEFAULT_VALUE, {{schema_name, SchemaName}, {field_name, FieldName}, {?DEFAULT_VALUE, NewDefaultValue}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply). 




%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec get_field_type(mb_session_id(), mb_schema_name(), mb_field_name()) -> ok | mb_error().
%-------------------------------------------------------------
get_field_type(SessionId, SchemaName, FieldName) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_GET_FIELD_TYPE, {{schema_name, SchemaName}, {field_name, FieldName}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).

%-------------------------------------------------------------
%
%-------------------------------------------------------------
-spec get_field_description(mb_session_id(), mb_schema_name(), mb_field_name()) -> ok | mb_error().
%-------------------------------------------------------------
get_field_description(SessionId, SchemaName, FieldName) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_GET_FIELD_DESCRIPTION, {{schema_name, SchemaName}, {field_name, FieldName}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).    


%-------------------------------------------------------------
%
%-------------------------------------------------------------
-spec get_field_label(mb_session_id(), mb_schema_name(), mb_field_name()) -> ok | mb_error().
%-------------------------------------------------------------
get_field_label(SessionId, SchemaName, FieldName) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_GET_FIELD_LABEL, {{schema_name, SchemaName}, {field_name, FieldName}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).  

%-------------------------------------------------------------
%
%-------------------------------------------------------------
-spec get_field_priority(mb_session_id(), mb_schema_name(), mb_field_name()) -> ok | mb_error().
%-------------------------------------------------------------
get_field_priority(SessionId, SchemaName, FieldName) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_GET_FIELD_PRIORITY, {{schema_name, SchemaName}, {field_name, FieldName}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply). 

%-------------------------------------------------------------
%
%-------------------------------------------------------------
-spec get_field_default_value(mb_session_id(), mb_schema_name(), mb_field_name()) -> ok | mb_error().
%-------------------------------------------------------------
get_field_default_value(SessionId, SchemaName, FieldName) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_GET_FIELD_DEFAULT_VALUE, {{schema_name, SchemaName}, {field_name, FieldName}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply). 

%-------------------------------------------------------------
%
%-------------------------------------------------------------
-spec get_field_position(mb_session_id(), mb_schema_name(), mb_field_name()) -> ok | mb_error().
%-------------------------------------------------------------
get_field_position(SessionId, SchemaName, FieldName) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_GET_FIELD_POSITION, {{schema_name, SchemaName}, {field_name, FieldName}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec get_field_attribute(mb_session_id(), mb_schema_name(), mb_field_name(), mb_field_attribute()) -> term() | mb_error().
%-------------------------------------------------------------
get_field_attribute(SessionId, SchemaName, FieldName, Attribute) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_GET_FIELD_ATTRIBUTE, {{schema_name, SchemaName}, {field_name, FieldName}, {field_attribute, Attribute}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec get_fields(mb_session_id(), mb_schema_name()) -> mb_field_spec_list() | mb_error().
%-------------------------------------------------------------
get_fields(SessionId, SchemaName) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_GET_FIELDS, {{schema_name, SchemaName}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec get_field(mb_session_id(), mb_schema_name(), mb_field_name()) -> mb_field_spec() | mb_error().
%-------------------------------------------------------------
get_field(SessionId, SchemaName, FieldName) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_GET_FIELD, {{schema_name, SchemaName}, {field_name, FieldName}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec get_field_count(mb_session_id(), mb_schema_name()) -> integer() | mb_error().
%-------------------------------------------------------------
get_field_count(SessionId, SchemaName) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_GET_FIELD_COUNT, {{schema_name, SchemaName}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec get_mandatory_field_count(mb_session_id(), mb_schema_name()) -> integer() | mb_error().
%-------------------------------------------------------------
get_mandatory_field_count(SessionId, SchemaName) ->
    Message = mb_ipc:build_request(SessionId, ?REQUEST_GET_MANDATORY_FIELD_COUNT, {{schema_name, SchemaName}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec get_field_names(mb_session_id(), mb_schema_name()) -> [] | [mb_field_name()] | mb_error().
%-------------------------------------------------------------
get_field_names(SessionId, SchemaName) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_GET_FIELD_NAMES, {{schema_name, SchemaName}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
read(SessionId, SchemaName, Key) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_READ_RECORD, {{schema_name, SchemaName}, {key_name, Key}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
select(SessionId, SchemaName, Field, Oper, Value) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_SELECT, {{schema_name, SchemaName}, {field_name, Field}, {operator, Oper}, {value, Value}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
select_or(SessionId, SchemaName, Field, Oper1, Value1, Oper2, Value2) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_SELECT_OR, {{schema_name, SchemaName}, {field_name, Field}, {operator1, Oper1}, {value1, Value1}, {operator2, Oper2}, {value2, Value2}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
select_and(SessionId, SchemaName, Field, Oper1, Value1, Oper2, Value2) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_SELECT_AND, {{schema_name, SchemaName}, {field_name, Field}, {operator1, Oper1}, {value1, Value1}, {operator2, Oper2}, {value2, Value2}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).



%-------------------------------------------------------------
% 
%-------------------------------------------------------------
add_record(SessionId, Record) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_ADD_RECORD, {{record, Record}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
add_record(SessionId, SchemaName, Record) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_ADD_RECORD, {{schema_name, SchemaName}, {record, Record}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
add_record(SessionId, SchemaName, Key, Data) -> 
    Message = mb_ipc:build_request(SessionId, ?REQUEST_ADD_RECORD, {{schema_name, SchemaName}, {key_name, Key}, {data, Data}}),
    Reply = mb_ipc:worker_call(SessionId, Message),
    request_response_result(Reply).

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
request_response_result({?PROT_VERSION, {{_SessionId, {?MSG_TYPE_REQUEST_RESPONSE, _}}, {result, Result}}}) -> Result;
request_response_result(Other) -> {error, {unrecognized_reply, Other}}.
