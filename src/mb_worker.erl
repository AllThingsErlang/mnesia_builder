-module(mb_worker).
-behaviour(gen_server).

-include("../include/mb_ipc.hrl").
-include("../include/mb_api.hrl").

-export([start/4, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(STATE_SERVER, server).
-define(STATE_SESSION_ID, session_id).
-define(STATE_SESSION_ACTIVE, session_active).
-define(STATE_CLIENT_PROCESS_REF, client_process_ref).
-define(STATE_TIMER, timer).
-define(STATE_SSG, specifications).
-define(STATE_MODULE, module).
-define(STATE_USE_MODULE, use_module).
-define(STATE_SESSION_DIR, session_dir).

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
start(ServerPid, ClientPid, Token, SessionDir) ->
    gen_server:start_link(?MODULE, {ServerPid, ClientPid, Token, SessionDir}, []).

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
init({ServerPid, ClientPid, Token, SessionRootDir}) ->
    io:format("[mb::worker::~p]: serving client ~p with token ~p~n", [self(), ClientPid, Token]),
    process_flag(trap_exit, true),
    TimerRef = erlang:start_timer(30000, self(), session_start_timer),

    SessionId = {self(),ClientPid,Token},
    SessionBinary = erlang:term_to_binary(SessionId),
    SessionMD5 = crypto:hash(md5, SessionBinary),
    SessionMD5Hex = lists:flatten(io_lib:format("~32.16.0b", [binary:decode_unsigned(SessionMD5)])),

    SessionDir = SessionRootDir ++ "/" ++ SessionMD5Hex,

    State =   #{?STATE_SERVER=>ServerPid, 
                ?STATE_SESSION_ID=>SessionId, 
                ?STATE_SESSION_ACTIVE=>false, 
                ?STATE_CLIENT_PROCESS_REF=>[],
                ?STATE_TIMER=>TimerRef, 
                ?STATE_MODULE=>[],
                ?STATE_USE_MODULE=>false,
                ?STATE_SESSION_DIR=>SessionDir,
                ?STATE_SSG=>mb_schemas:new()},

    case file:make_dir(SessionDir) of
        ok -> {ok, State};
        {error, eexist} -> {stop, {error, {session_dir_exists, SessionDir}}};
        {error, Reason} -> {stop, {error, {failed_to_create_session_dir, Reason}}}
    end.


%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
handle_call({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, MessageId}}, Payload}}, From, State) ->

    ReceivedPid = mb_ipc:remove_pid_alias(From),
    io:format("[mb::worker::~p]: received ~p from ~p~n", [self(), {?MSG_TYPE_REQUEST, MessageId}, ReceivedPid]),

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
                            io:format("[mb::worker::~p]: message rejected, unexpected_message ~n", [self()]),
                            ReplyMessage = mb_ipc:build_end_session_response(SessionId, {unexpected_message, {MessageId, {?STATE_SESSION_ACTIVE, maps:get(?STATE_SESSION_ACTIVE, State)}}}),
                            {reply, ReplyMessage, State}
                    end;

                false ->
                    io:format("[mb::worker::~p]: message rejected, invalid_session_credentials, unrecognized_sender ~n", [self()]),
                    ReplyMessage = mb_ipc:build_end_session_response(SessionId, {invalid_session_credentials, unrecognized_sender}),
                    {reply, ReplyMessage, State}
            end;

        _ -> 
            io:format("[mb::worker::~p]: message rejected, invalid_session_credentials, unrecognized_session_id ~n", [self()]),
            ReplyMessage = mb_ipc:build_end_session_response(SessionId, {invalid_session_credentials, unrecognized_session_id}),
            {reply, ReplyMessage, State}
    end;

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
handle_call(Request, _From, State) ->
    io:format("[mb::worker::~p]: unsupported_request: ~p~n", [self(), Request]),

    ReplyMessage = mb_ipc:build_error(unsupported_request),
    {reply, ReplyMessage, State}.

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_START_SESSION}}, {}}}, State) ->

    io:format("[mb::worker::~p]: ~p accepted, cancelling timer~n", [self(), ?REQUEST_START_SESSION]),

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

    io:format("[mb::worker::~p]: ~p accepted~n", [self(), ?REQUEST_END_SESSION]),
    UpdatedState = maps:update(?STATE_SESSION_ACTIVE, false, State),
    {stop, normal, ok, UpdatedState};

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_NEW_SSG}}, {}}}, State) ->

    UpdatedState = maps:update(?STATE_SSG, mb_schemas:new(), State),
    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_NEW_SSG, ok),
    {reply, ReplyMessage, UpdatedState};

    

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_NEW_SSG}}, {{name, Name}, {owner, Owner}, {email, Email}, {description, Description}}}}, State) ->

    UpdatedState = maps:update(?STATE_SSG, mb_schemas:new(Name, Owner, Email, Description), State),
    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_NEW_SSG, ok),
    {reply, ReplyMessage, UpdatedState};


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_GET_SSG}}, {}}}, State) ->

    Result = {ok, maps:get(?STATE_SSG, State)},
    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_GET_SSG, Result),
    {reply, ReplyMessage, State};


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_SET_SSG_NAME}}, {{name, Name}}}}, State) ->

    SSG =  maps:get(?STATE_SSG, State),

    case mb_schemas:set_ssg_name(Name, SSG) of 
        {error, Reason} -> 
            UpdatedState = State,
            Result = {error, Reason};

        UpdatedSSG ->
            UpdatedState = maps:update(?STATE_SSG, UpdatedSSG, State),
            Result = ok
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_SET_SSG_NAME, Result),
    {reply, ReplyMessage, UpdatedState};

    
%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_SET_SSG_OWNER}}, {{owner, Owner}}}}, State) ->

    SSG =  maps:get(?STATE_SSG, State),

    case mb_schemas:set_ssg_owner(Owner, SSG) of 
        {error, Reason} -> 
            UpdatedState = State,
            Result = {error, Reason};

        UpdatedSSG ->
            UpdatedState = maps:update(?STATE_SSG, UpdatedSSG, State),
            Result = ok
    end,
    
    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_SET_SSG_OWNER, Result),
    {reply, ReplyMessage, UpdatedState};


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_SET_SSG_EMAIL}}, {{email, Email}}}}, State) ->

    SSG =  maps:get(?STATE_SSG, State),

    case mb_schemas:set_ssg_email(Email, SSG) of 
        {error, Reason} -> 
            UpdatedState = State,
            Result = {error, Reason};

        UpdatedSSG ->
            UpdatedState = maps:update(?STATE_SSG, UpdatedSSG, State),
            Result = ok
    end,
    
    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_SET_SSG_EMAIL, Result),
    {reply, ReplyMessage, UpdatedState};


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_SET_SSG_DESCRIPTION}}, {{description, Description}}}}, State) ->

    SSG =  maps:get(?STATE_SSG, State),

    case mb_schemas:set_ssg_description(Description, SSG) of 
        {error, Reason} -> 
            UpdatedState = State,
            Result = {error, Reason};

        UpdatedSSG ->
            UpdatedState = maps:update(?STATE_SSG, UpdatedSSG, State),
            Result = ok
    end,
    
    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_SET_SSG_DESCRIPTION, Result),
    {reply, ReplyMessage, UpdatedState};


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_SET_MODULE_NAME}}, {{module_name, Module}}}}, State) ->

    case is_atom(Module) of 
        true -> 
            UpdatedState = maps:update(?STATE_MODULE, Module, State),
            Result = ok;

        false -> 
            UpdatedState = State,
            Result = {error, {invalid_module_name, Module}}
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_SET_MODULE_NAME, Result),
    {reply, ReplyMessage, UpdatedState};

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_UPLOAD_MODULE}}, {{module_name, Module}, {module, Binary}}}}, State) ->

    % 1. Validate module name
    % 2. Create module file and write the source code to file
    % 3. Compile module
    % 4. Load module
    % 5. Retrieve the new SSG
    % 6. Update State
    % 7. Reply

    case is_atom(Module) of 
        true -> 
            SessionDir = maps:get(?STATE_SESSION_DIR, State),
            File = SessionDir ++ "/" ++ atom_to_list(Module) ++ ".erl",

            case file:write_file(File, Binary) of
                ok -> 
                    case compile:file(File, [report_errors]) of
                        {ok, Module} -> 
                            case code:load_file(Module) of 
                                {module, Module} -> 
                                    NewSSG = Module:get_ssg(),
                                    UpdatedState = maps:update(?STATE_MODULE, Module, maps:update(?STATE_SSG, NewSSG, State)),
                                    Result = ok;

                                {error, Reason} -> 
                                    UpdatedState = State,
                                    Result = {error, Reason}
                            end;

                        {error, Errors} -> 
                            UpdatedState = State,
                            Result = {error, Errors}
                    end;

                {error, Reason} ->
                    UpdatedState = State,
                    Result = {error, Reason}
            end;

        false ->
            UpdatedState = State,
            Result = {error, {invalid_module_name, Module}}
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_UPLOAD_MODULE, Result),
    {reply, ReplyMessage, UpdatedState};

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_DOWNLOAD_MODULE}}, {}}}, State) ->

    case maps:get(?STATE_MODULE, State) of 
        [] -> Result = {error, module_not_defined};
        Module ->

            SessionDir = maps:get(?STATE_SESSION_DIR, State),
            SrcFile = SessionDir ++ "/" ++ atom_to_list(Module) ++ ".erl",
            HrlFile = SessionDir ++ "/" ++ atom_to_list(Module) ++ ".hrl",

            % Generate to make sure that the source and header files are in place.
            SSG = maps:get(?STATE_SSG, State),
            case mb_schemas:generate(Module, SessionDir, SessionDir, SSG) of
                ok -> 
                    case file:read_file(SrcFile) of
                        {ok, SrcBinary} -> 
                            
                            case file:read_file(HrlFile) of
                                {ok, HrlBinary} -> Result = {ok, {Module, SrcBinary, HrlBinary}};
                                {error, Reason} -> Result = {error, {Reason, HrlFile}}
                            end;

                        {error, Reason} -> Result = {error, {Reason, SrcFile}}
                    end;
                {error, Reason} -> Result = {error, Reason}
            end
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_DOWNLOAD_MODULE, Result),
    {reply, ReplyMessage, State};

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_GENERATE}}, {}}}, State) ->

    case maps:get(?STATE_MODULE, State) of  
        [] -> Result = {error, module_name_not_defined};
        Module ->
            SessionDir = maps:get(?STATE_SESSION_DIR, State),
            SSG =  maps:get(?STATE_SSG, State),

            case mb_schemas:generate(Module, SessionDir, SessionDir, SSG) of
                ok -> 
                    case compile:file(SessionDir ++ "/" ++ atom_to_list(Module) ++ ".erl", [report_errors]) of
                        {ok, Module} -> Result = ok;
                        {error, Errors} -> Result = {error, Errors}
                    end;

                {error, Reason} -> Result = {error, Reason}
            end
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_GENERATE, Result),
    {reply, ReplyMessage, State};


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_USE_MODULE}}, {{use_module, true}}}}, State) ->

    case maps:get(?STATE_MODULE, State) of
        [] -> 
            UpdatedState = State,
            Result = {error, module_name_not_set};

        Module ->
            SessionDir = maps:get(?STATE_SESSION_DIR, State),
            code:add_pathz(SessionDir),
                            
            case code:load_file(Module) of 
                {module, Module} -> 
                    UpdatedState = maps:update(?STATE_USE_MODULE, true, State),
                    Result = ok;
                {error, Reason} -> 
                    UpdatedState = State,
                    Result = {error, Reason}
            end
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_USE_MODULE, Result),
    {reply, ReplyMessage, UpdatedState};


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_USE_MODULE}}, {{use_module, false}}}}, State) ->

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_USE_MODULE, ok),
    {reply, ReplyMessage, maps:update(?STATE_USE_MODULE, false, State)};


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_ADD_SCHEMA}}, {{schema_name, SchemaName}}}}, State) ->

    SSG = maps:get(?STATE_SSG, State),
    case mb_schemas:add_schema(SchemaName, SSG) of 
        {error, Reason} -> 
            UpdatedState = State,
            Result = {error, Reason};

        UpdatedSS ->
            UpdatedState = maps:update(?STATE_SSG, UpdatedSS, State),
            Result = ok 
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_ADD_SCHEMA, Result),
    {reply, ReplyMessage, UpdatedState};


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_DELETE_SCHEMA}}, {{schema_name, SchemaName}}}}, State) ->

    SSG = maps:get(?STATE_SSG, State),
    UpdatedSS = mb_schemas:delete_schema(SchemaName, SSG),
    UpdatedState = maps:update(?STATE_SSG, UpdatedSS, State),
    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_DELETE_SCHEMA, ok),
    {reply, ReplyMessage, UpdatedState};

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_GET_SCHEMA}}, {{schema_name, SchemaName}}}}, State) ->

    SSG = maps:get(?STATE_SSG, State),
    
    case mb_schemas:get_schema(SchemaName, SSG) of 
        {error, Reason} -> Result = {error, Reason};
        Schema -> Result = {ok, Schema}
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_GET_SCHEMA, Result),
    {reply, ReplyMessage, State};

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_GET_ALL_SCHEMAS}}, {}}}, State) ->

    SSG = maps:get(?STATE_SSG, State),
    
    case mb_schemas:schemas(SSG) of 
        {error, Reason} -> Result = {error, Reason};
        SchemasList -> Result = {ok, SchemasList}
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_GET_ALL_SCHEMAS, Result),
    {reply, ReplyMessage, State};

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_SET_SCHEMA_ATTRIBUTES}}, {{schema_name, SchemaName}, {schema_avp_list, SchemaAvpList}}}}, State) ->

    SSG = maps:get(?STATE_SSG, State),
    
    case mb_schemas:set_schema_attributes(SchemaAvpList, SchemaName, SSG) of 
        {error, Reason} -> 
            UpdatedState = State,
            Result = {error, Reason};

        UpdatedSS -> 
            UpdatedState = maps:update(?STATE_SSG, UpdatedSS, State),
            Result = ok
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_SET_SCHEMA_ATTRIBUTES, Result),
    {reply, ReplyMessage, UpdatedState};

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_GET_SCHEMA_ATTRIBUTE}}, {{schema_name, SchemaName}, {schema_attribute, Attribute}}}}, State) ->

    SSG = maps:get(?STATE_SSG, State),
    
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

    case use_module(State) of 
        {ok, Module} -> Result = {ok, Module:schema_names()};
        {error, Reason} -> Result = {error, Reason};
        false -> 
            SSG = maps:get(?STATE_SSG, State),
            Result = {ok, mb_schemas:schema_names(SSG)}
    end,
    
    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_GET_SCHEMA_NAMES, Result),
    {reply, ReplyMessage, State};


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_ADD_FIELD}}, {{schema_name, SchemaName}, {field_name, FieldName}}}}, State) ->

    SSG = maps:get(?STATE_SSG, State),
    
    case mb_schemas:add_field(FieldName, SchemaName, SSG) of 
        {error, Reason} -> 
            UpdatedState = State,
            Result = {error, Reason};

        UpdatedSS -> 
            UpdatedState = maps:update(?STATE_SSG, UpdatedSS, State),
            Result = ok 
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_ADD_FIELD, Result),
    {reply, ReplyMessage, UpdatedState};


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_SET_FIELD_ATTRIBUTES}}, {{schema_name, SchemaName}, {field_name, FieldName}, {field_avp_list, FieldAvpList}}}}, State) ->

    SSG = maps:get(?STATE_SSG, State),
    
    case mb_schemas:set_field_attributes(FieldAvpList, FieldName, SchemaName, SSG) of 
        {error, Reason} -> 
            UpdatedState = State,
            Result = {error, Reason};

        UpdatedSS -> 
            UpdatedState = maps:update(?STATE_SSG, UpdatedSS, State),
            Result = ok
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_SET_SCHEMA_ATTRIBUTES, Result),
    {reply, ReplyMessage, UpdatedState};

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_GET_FIELD_ATTRIBUTE}}, {{schema_name, SchemaName}, {field_name, FieldName}, {field_attribute, Attribute}}}}, State) ->

    case use_module(State) of 
        {ok, Module} -> 
            case Module:get_field_attribute(Attribute, FieldName, SchemaName) of
                {error, Reason} -> Result = {error, Reason};
                Value -> Result = {ok, Value}
            end;

        {error, Reason} -> Result = {error, Reason};
        false -> 
            SSG = maps:get(?STATE_SSG, State),
    
            case mb_schemas:get_field_attribute(Attribute, FieldName, SchemaName, SSG) of 
                {error, Reason} -> Result = {error, Reason};
                Value -> Result = {ok, Value}
            end
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_GET_FIELD_ATTRIBUTE, Result),
    {reply, ReplyMessage, State};

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_GET_FIELDS}}, {{schema_name, SchemaName}}}}, State) ->

    case use_module(State) of 
        {ok, Module} -> 
            case Module:fields(SchemaName) of
                {error, Reason} -> Result = {error, Reason};
                Fields -> Result = {ok, Fields}
            end;

        {error, Reason} -> Result = {error, Reason};
        false -> 
            SSG = maps:get(?STATE_SSG, State),
    
            case mb_schemas:fields(SchemaName, SSG) of 
                {error, Reason} -> Result = {error, Reason};
                Fields -> Result = {ok, Fields}
            end
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_GET_FIELDS, Result),
    {reply, ReplyMessage, State};

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_GET_FIELD}}, {{schema_name, SchemaName}, {field_name, FieldName}}}}, State) ->

    case use_module(State) of 
        {ok, Module} -> 
            case Module:get_field(FieldName, SchemaName) of
                {error, Reason} -> Result = {error, Reason};
                FieldSpec -> Result = {ok, FieldSpec}
            end;

        {error, Reason} -> Result = {error, Reason};
        false -> 
            SSG = maps:get(?STATE_SSG, State),
    
            case mb_schemas:get_field(FieldName, SchemaName, SSG) of 
                {error, Reason} -> Result = {error, Reason};
                FieldSpec -> Result = {ok, FieldSpec}
            end
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_GET_FIELD, Result),
    {reply, ReplyMessage, State};

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_GET_FIELD_COUNT}}, {{schema_name, SchemaName}}}}, State) ->

    case use_module(State) of 
        {ok, Module} -> 
            case Module:field_count(SchemaName) of
                {error, Reason} -> Result = {error, Reason};
                Count -> Result = {ok, Count}
            end;

        {error, Reason} -> Result = {error, Reason};
        false -> 
            SSG = maps:get(?STATE_SSG, State),
    
            case mb_schemas:field_count(SchemaName, SSG) of 
                {error, Reason} -> Result = {error, Reason};
                Count -> Result = {ok, Count}
            end
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_GET_FIELD_COUNT, Result),
    {reply, ReplyMessage, State};

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_GET_MANDATORY_FIELD_COUNT}}, {{schema_name, SchemaName}}}}, State) ->

    case use_module(State) of 
        {ok, Module} -> 
            case Module:mandatory_field_count(SchemaName) of
                {error, Reason} -> Result = {error, Reason};
                Count -> Result = {ok, Count}
            end;

        {error, Reason} -> Result = {error, Reason};
        false -> 
            SSG = maps:get(?STATE_SSG, State),
    
            case mb_schemas:mandatory_field_count(SchemaName, SSG) of 
                {error, Reason} -> Result = {error, Reason};
                Count -> Result = {ok, Count}
            end
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_GET_MANDATORY_FIELD_COUNT, Result),
    {reply, ReplyMessage, State};


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_GET_FIELD_NAMES}}, {{schema_name, SchemaName}}}}, State) ->

    case use_module(State) of 
        {ok, Module} -> 
            case Module:field_names(SchemaName) of
                {error, Reason} -> Result = {error, Reason};
                FieldNames -> Result = {ok, FieldNames}
            end;

        {error, Reason} -> Result = {error, Reason};
        false -> 
            SSG = maps:get(?STATE_SSG, State),
    
            case mb_schemas:field_names(SchemaName, SSG) of 
                {error, Reason} -> Result = {error, Reason};
                FieldNames -> Result = {ok, FieldNames}
            end
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_GET_FIELD_NAMES, Result),
    {reply, ReplyMessage, State};

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_GET_FIELD_POSITION}}, {{schema_name, SchemaName}, {field_name, FieldName}}}}, State) ->

    case use_module(State) of 
        {ok, Module} -> 
            case Module:field_position(FieldName, SchemaName) of
                {error, Reason} -> Result = {error, Reason};
                Position -> Result = {ok, Position}
            end;

        {error, Reason} -> Result = {error, Reason};
        false -> 
            SSG = maps:get(?STATE_SSG, State),
    
            case mb_schemas:field_position(FieldName, SchemaName, SSG) of 
                {error, Reason} -> Result = {error, Reason};
                Position -> Result = {ok, Position}
            end
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_GET_FIELD_POSITION, Result),
    {reply, ReplyMessage, State};



%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_READ_RECORD}}, {{schema_name, SchemaName}, {key_name, Key}}}}, State) ->

    case use_module(State) of 
        {ok, Module} -> 
            case Module:read(SchemaName, Key) of
                {error, Reason} -> Result = {error, Reason};
                RecordList -> Result = {ok, RecordList}
            end;

        {error, Reason} -> Result = {error, Reason};
        false ->
            SSG = maps:get(?STATE_SSG, State),

            case mb_schemas:read(Key, SchemaName, SSG) of 
                {error, Reason} -> Result = {error, Reason};
                RecordList -> Result = {ok, RecordList}
            end
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_READ_RECORD, Result),
    {reply, ReplyMessage, State};



%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_SELECT}}, {{schema_name, SchemaName}, {field_name, Field}, {operator, Oper}, {value, Value}}}}, State) ->

    case use_module(State) of 
        {ok, Module} -> 
            case Module:select(SchemaName, Field, Oper, Value) of
                {error, Reason} -> Result = {error, Reason};
                RecordList -> Result = {ok, RecordList}
            end;

        {error, Reason} -> Result = {error, Reason};
        false -> 
            SSG = maps:get(?STATE_SSG, State),
    
            case mb_schemas:select(SchemaName, Field, Oper, Value, SSG) of 
                {error, Reason} -> Result = {error, Reason};
                RecordList -> Result = {ok, RecordList}
            end
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_SELECT, Result),
    {reply, ReplyMessage, State};



%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_SELECT_OR}}, {{schema_name, SchemaName}, {field_name, Field}, {operator1, Oper1}, {value1, Value1}, {operator2, Oper2}, {value2, Value2}}}}, State) ->

    case use_module(State) of 
        {ok, Module} -> 
            case Module:select_or(SchemaName, Field, Oper1, Value1, Oper2, Value2) of
                {error, Reason} -> Result = {error, Reason};
                RecordList -> Result = {ok, RecordList}
            end;

        {error, Reason} -> Result = {error, Reason};
        false -> 
            SSG = maps:get(?STATE_SSG, State),
    
            case mb_schemas:select_or(SchemaName, Field, Oper1, Value1, Oper2, Value2, SSG) of 
                {error, Reason} -> Result = {error, Reason};
                RecordList -> Result = {ok, RecordList}
            end
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_SELECT_OR, Result),
    {reply, ReplyMessage, State};



%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_SELECT_AND}}, {{schema_name, SchemaName}, {field_name, Field}, {operator1, Oper1}, {value1, Value1}, {operator2, Oper2}, {value2, Value2}}}}, State) ->

    case use_module(State) of 
        {ok, Module} -> 
            case Module:select_and(SchemaName, Field, Oper1, Value1, Oper2, Value2) of
                {error, Reason} -> Result = {error, Reason};
                RecordList -> Result = {ok, RecordList}
            end;

        {error, Reason} -> Result = {error, Reason};
        false -> 
            SSG = maps:get(?STATE_SSG, State),
    
            case mb_schemas:select_and(SchemaName, Field, Oper1, Value1, Oper2, Value2, SSG) of 
                {error, Reason} -> Result = {error, Reason};
                RecordList -> Result = {ok, RecordList}
            end
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_SELECT_AND, Result),
    {reply, ReplyMessage, State};



%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_ADD_RECORD}}, {{record, Record}}}}, State) ->

    case use_module(State) of 
        {ok, Module} -> 
            case Module:add(Record) of
                {error, Reason} -> Result = {error, Reason};
                Result -> ok
            end;

        {error, Reason} -> Result = {error, Reason};
        false -> 
            SSG = maps:get(?STATE_SSG, State),
    
            case mb_schemas:add(Record, SSG) of 
                {error, Reason} -> Result = {error, Reason};
                Result -> ok
            end
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_ADD_RECORD, Result),
    {reply, ReplyMessage, State};



%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_ADD_RECORD}}, {{schema_name, SchemaName}, {record, Record}}}}, State) ->

    case use_module(State) of 
        {ok, Module} -> 
            case Module:add(SchemaName, Record) of
                {error, Reason} -> Result = {error, Reason};
                Result -> ok
            end;

        {error, Reason} -> Result = {error, Reason};
        false -> 
            SSG = maps:get(?STATE_SSG, State),
    
            case mb_schemas:add(SchemaName, Record, SSG) of 
                {error, Reason} -> Result = {error, Reason};
                Result -> ok
            end
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_ADD_RECORD, Result),
    {reply, ReplyMessage, State};



%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_ADD_RECORD}}, {{schema_name, SchemaName}, {key_name, Key}, {data, Data}}}}, State) ->

    case use_module(State) of 
        {ok, Module} -> 
            case Module:add(SchemaName, Key, Data) of
                {error, Reason} -> Result = {error, Reason};
                Result -> ok
            end;

        {error, Reason} -> Result = {error, Reason};
        false -> 
            SSG = maps:get(?STATE_SSG, State),
    
            case mb_schemas:add(SchemaName, Key, Data, SSG) of 
                {error, Reason} -> Result = {error, Reason};
                Result -> ok
            end
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_ADD_RECORD, Result),
    {reply, ReplyMessage, State}.



%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_cast(shutdown, State) ->
    io:format("[mb::worker::~p]: handle_cast: shutdown~n", [self()]),
    {stop, normal, State};

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_cast(Message, State) ->
    io:format("[mb::worker::~p]: unsupported message: ~p~n", [self(), Message]),
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
                    io:format("[mb::worker::~p]: unknown timer reference ~p~n", [self(), IncomingTimerRef]),
                    {noreply, State}
            end 
    end;

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
handle_info({'EXIT', _FromPid, Reason}, State) ->
    io:format("[mb::worker::~p]: received exit signal, reason: ~p~n", [self(), Reason]),
    {stop, Reason, State};

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_info({'DOWN', _Ref, process, ClientPid, _Reason}, State) ->
    io:format("[mb::worker::~p]: client disconnected~n", [self()]),

    % Handle client disconnection
    SessionId = maps:get(?STATE_SESSION_ID, State),

    case SessionId of
        {_, ClientPid, _} ->
            io:format("[mb::worker::~p]:  client ~p disconnected. Stopping worker.~n", [self(), ClientPid]),
            {stop, normal, State};
        _ ->
            io:format("[mb::worker::~p]:  no associated session id found: ~p.~n", [self(), SessionId]),
            {noreply, State}
    end;

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_info(Info, State) ->
    io:format("[mb::worker::~p]: unsupported info notification: ~p~n", [self(), Info]),
    {noreply, State}.


%-------------------------------------------------------------
%-------------------------------------------------------------
terminate(shutdown, _State) -> ok;

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
terminate(_Reason, State) ->
    io:format("[mb::worker::~p]: terminated for session ~p~n", [self(), maps:get(?STATE_SESSION_ID, State)]),
    ok.

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%-------------------------------------------------------------
%-------------------------------------------------------------
use_module(State) ->
    case maps:get(?STATE_USE_MODULE, State) of
        true ->
            Module = maps:get(?STATE_MODULE, State),
            case code:module_loaded(Module) of
                true ->  {ok, Module};
                false -> {error, module_not_loaded} 
            end;
        false -> false
    end.