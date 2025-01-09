-module(mb_worker).
-behaviour(gen_server).

-include("../include/mb_ipc.hrl").
-include("../include/mb_api.hrl").
-include("../include/mb.hrl").

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

-define(DEFAULT_SSG_NAME, not_defined).

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
                ?STATE_SSG=>mb_ssg:new()},

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

    NewSSG =  maps:update(?NAME, ?DEFAULT_SSG_NAME, mb_ssg:new()),
    UpdatedState = update_state_new_ssg(State, NewSSG),
    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_NEW_SSG, ok),
    {reply, ReplyMessage, UpdatedState};

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_NEW_SSG}}, {{name, Name}, {owner, Owner}, {email, Email}, {description, Description}}}}, State) ->

    case mb_ssg:new(Name, Owner, Email, Description) of 
        {error, Reason} -> 
            UpdatedState = State,
            Result = {error, Reason};
        NewSSG ->
            UpdatedState = update_state_new_ssg(State, NewSSG),
            Result = ok 
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_NEW_SSG, Result),
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

    case maps:get(?NAME, SSG) == Name of 
        false -> 
            case mb_ssg:set_ssg_name(Name, SSG) of 
                {error, Reason} -> 
                    UpdatedState = State,
                    Result = {error, Reason};

                UpdatedSSG ->

                    case mb_tables:assign_self(UpdatedSSG) of 
                        ok -> 
                            UpdatedStateInterim = maps:update(?STATE_SSG, UpdatedSSG, State),
                            % The module name is always the SSG name
                            UpdatedState = maps:update(?STATE_MODULE, Name, UpdatedStateInterim),
                            Result = ok;
                        {error, Reason} -> 
                            UpdatedState = State,
                            Result = {error, Reason}
                    end
            end;

        true -> 
            UpdatedState = State,
            Result = ok
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_SET_SSG_NAME, Result),
    {reply, ReplyMessage, UpdatedState};

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_SET_SSG_OWNER}}, {{owner, Owner}}}}, State) ->

    SSG =  maps:get(?STATE_SSG, State),

    case mb_ssg:set_ssg_owner(Owner, SSG) of 
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

    case mb_ssg:set_ssg_email(Email, SSG) of 
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

    case mb_ssg:set_ssg_description(Description, SSG) of 
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
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_UPLOAD_MODULE}}, {{module_name, Module}, {module, Binary}, {force_load, ForceLoadFlag}}}}, State) ->

    % 1. Validate module name
    % 2. Create module file and write the source code to file
    % 3. Compile module
    % 4. Load module
    % 5. Validate callback and retrieve the new SSG
    % 6. Update State and set the Module name to the new SSG name
    % 7. Reply

    case is_atom(Module) of 
        true -> 
            {SrcDir, _IncDir, EbinDir} = get_client_directories(),
            File = SrcDir ++ "/" ++ atom_to_list(Module) ++ ".erl",

            case file:write_file(File, Binary) of
                ok -> 
                    case mb_utilities:compile_and_load(Module, File, EbinDir) of 
                        ok -> 
                            case erlang:function_exported(Module, get_ssg, 0) of
                                true ->
                                    NewSSG = Module:get_ssg(),
                                    case mb_ssg:validate_ssg(NewSSG) of 
                                        [] ->
                                            UpdatedState = maps:update(?STATE_MODULE, Module, maps:update(?STATE_SSG, NewSSG, State)),
                                            Result = ok;
                                        Errors -> 
                                            case ForceLoadFlag of 
                                                true -> 
                                                    UpdatedState = maps:update(?STATE_MODULE, Module, maps:update(?STATE_SSG, NewSSG, State)),
                                                    Result = ok;
                                                _ ->
                                                    UpdatedState = State,
                                                    Result = {error, {invalid_specifications, Errors}}
                                            end
                                    end;
                                false -> 
                                    UpdatedState = State,
                                    Result = {error, invalid_module_callback}
                            end;
                        {error, Reason} ->
                            UpdatedState = State,
                            Result = {error, Reason}
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

            {SrcDir, IncDir, _EbinDir} = get_client_directories(),

            SrcFile = SrcDir ++ "/" ++ atom_to_list(Module) ++ ".erl",
            HrlFile = IncDir ++ "/" ++ atom_to_list(Module) ++ ".hrl",

            % Generate to make sure that the source and header files are in place.
            SSG = maps:get(?STATE_SSG, State),
            case mb_ssg:generate(Module, SrcDir, IncDir, SSG) of
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
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_VALIDATE_SSG}}, {}}}, State) ->

    SSG = maps:get(?STATE_SSG, State),
    Result = mb_ssg:validate_ssg(SSG),
    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_VALIDATE_SSG, Result),
    {reply, ReplyMessage, State};

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_GENERATE}}, {}}}, State) ->

    case maps:get(?STATE_MODULE, State) of  
        [] -> Result = {error, module_name_not_defined};
        Module ->
            SSG =  maps:get(?STATE_SSG, State),

            {SrcDir, IncDir, EbinDir} = get_client_directories(),

            case mb_ssg:generate(Module, SrcDir, IncDir, SSG) of
                ok -> 
                    case compile:file(SrcDir ++ "/" ++ atom_to_list(Module) ++ ".erl", [{outdir, EbinDir}, report_errors]) of
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
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_DEPLOY}}, {}}}, State) ->

    case use_module(State) of 

        {ok, Module} -> 
            io:format("attempting to deploy using ~p~n", [Module]),

            case Module:deploy() of
                {error, Reason} -> Result = {error, Reason};
                Result -> ok
            end;

        {error, Reason} -> Result = {error, Reason};
        false -> 
            SSG = maps:get(?STATE_SSG, State),
    
            io:format("attempting to deploy using mb_db_management~n"),

            case mb_db_management:deploy(SSG) of 
                {error, Reason} -> Result = {error, Reason};
                Result -> ok
            end
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_DEPLOY, Result),
    {reply, ReplyMessage, State};

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_USE_MODULE}}, {{use_module, true}}}}, State) ->

    case load_module(State) of 
        {error, Reason} -> 
            UpdatedState = maps:update(?STATE_USE_MODULE, false, State),
            Result = {error, Reason};

        _Module ->
            UpdatedState = maps:update(?STATE_USE_MODULE, true, State),
            Result = ok 
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
    case mb_ssg:add_schema(SchemaName, SSG) of 
        {error, Reason} -> 
            UpdatedState = State,
            Result = {error, Reason};

        UpdatedSSG ->
            UpdatedState = maps:update(?STATE_SSG, UpdatedSSG, State),
            Result = ok 
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_ADD_SCHEMA, Result),
    {reply, ReplyMessage, UpdatedState};


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_DELETE_SCHEMA}}, {{schema_name, SchemaName}}}}, State) ->

    SSG = maps:get(?STATE_SSG, State),
    UpdatedSSG = mb_ssg:delete_schema(SchemaName, SSG),
    UpdatedState = maps:update(?STATE_SSG, UpdatedSSG, State),
    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_DELETE_SCHEMA, ok),
    {reply, ReplyMessage, UpdatedState};

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_GET_SCHEMA}}, {{schema_name, SchemaName}}}}, State) ->

    SSG = maps:get(?STATE_SSG, State),
    
    case mb_ssg:get_schema(SchemaName, SSG) of 
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
    
    case mb_ssg:schemas(SSG) of 
        {error, Reason} -> Result = {error, Reason};
        SchemasList -> Result = {ok, SchemasList}
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_GET_ALL_SCHEMAS, Result),
    {reply, ReplyMessage, State};






%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_ADD_SERVER_NODE}}, {{schema_name, SchemaName}, {?RAM_COPIES, []}}}}, State) ->

    SSG = maps:get(?STATE_SSG, State),
    
    case mb_ssg:add_schema_ram_copies([node()], SchemaName, SSG) of 
        {error, Reason} -> 
            UpdatedState = State,
            Result = {error, Reason};

        UpdatedSSG -> 
            UpdatedState = maps:update(?STATE_SSG, UpdatedSSG, State),
            Result = ok
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_ADD_SERVER_NODE, Result),
    {reply, ReplyMessage, UpdatedState};

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_ADD_SERVER_NODE}}, {{schema_name, SchemaName}, {?DISC_COPIES, []}}}}, State) ->

    SSG = maps:get(?STATE_SSG, State),
    
    case mb_ssg:add_schema_disc_copies([node()], SchemaName, SSG) of 
        {error, Reason} -> 
            UpdatedState = State,
            Result = {error, Reason};

        UpdatedSSG -> 
            UpdatedState = maps:update(?STATE_SSG, UpdatedSSG, State),
            Result = ok
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_ADD_SERVER_NODE, Result),
    {reply, ReplyMessage, UpdatedState};

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_ADD_SERVER_NODE}}, {{schema_name, SchemaName}, {?DISC_ONLY_COPIES, []}}}}, State) ->

    SSG = maps:get(?STATE_SSG, State),
    
    case mb_ssg:add_schema_disc_only_copies([node()], SchemaName, SSG) of 
        {error, Reason} -> 
            UpdatedState = State,
            Result = {error, Reason};

        UpdatedSSG -> 
            UpdatedState = maps:update(?STATE_SSG, UpdatedSSG, State),
            Result = ok
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_ADD_SERVER_NODE, Result),
    {reply, ReplyMessage, UpdatedState};







%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_ADD_REST_OF_CLUSTER}}, {{schema_name, SchemaName}, {?RAM_COPIES, []}}}}, State) ->

    SSG = maps:get(?STATE_SSG, State),
    
    case mb_ssg:add_schema_ram_copies(nodes(), SchemaName, SSG) of 
        {error, Reason} -> 
            UpdatedState = State,
            Result = {error, Reason};

        UpdatedSSG -> 
            UpdatedState = maps:update(?STATE_SSG, UpdatedSSG, State),
            Result = ok
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_ADD_REST_OF_CLUSTER, Result),
    {reply, ReplyMessage, UpdatedState};

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_ADD_REST_OF_CLUSTER}}, {{schema_name, SchemaName}, {?DISC_COPIES, []}}}}, State) ->

    SSG = maps:get(?STATE_SSG, State),
    
    case mb_ssg:add_schema_disc_copies(nodes(), SchemaName, SSG) of 
        {error, Reason} -> 
            UpdatedState = State,
            Result = {error, Reason};

        UpdatedSSG -> 
            UpdatedState = maps:update(?STATE_SSG, UpdatedSSG, State),
            Result = ok
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_ADD_REST_OF_CLUSTER, Result),
    {reply, ReplyMessage, UpdatedState};

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_ADD_REST_OF_CLUSTER}}, {{schema_name, SchemaName}, {?DISC_ONLY_COPIES, []}}}}, State) ->

    SSG = maps:get(?STATE_SSG, State),
    
    case mb_ssg:add_schema_disc_only_copies(nodes(), SchemaName, SSG) of 
        {error, Reason} -> 
            UpdatedState = State,
            Result = {error, Reason};

        UpdatedSSG -> 
            UpdatedState = maps:update(?STATE_SSG, UpdatedSSG, State),
            Result = ok
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_ADD_REST_OF_CLUSTER, Result),
    {reply, ReplyMessage, UpdatedState};





%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_ADD_NODES}}, {{schema_name, SchemaName}, {?RAM_COPIES, NodesList}}}}, State) ->

    SSG = maps:get(?STATE_SSG, State),
    
    case mb_ssg:add_schema_ram_copies(NodesList, SchemaName, SSG) of 
        {error, Reason} -> 
            UpdatedState = State,
            Result = {error, Reason};

        UpdatedSSG -> 
            UpdatedState = maps:update(?STATE_SSG, UpdatedSSG, State),
            Result = ok
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_ADD_NODES, Result),
    {reply, ReplyMessage, UpdatedState};

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_ADD_NODES}}, {{schema_name, SchemaName}, {?DISC_COPIES, NodesList}}}}, State) ->

    SSG = maps:get(?STATE_SSG, State),
    
    case mb_ssg:add_schema_disc_copies(NodesList, SchemaName, SSG) of 
        {error, Reason} -> 
            UpdatedState = State,
            Result = {error, Reason};

        UpdatedSSG -> 
            UpdatedState = maps:update(?STATE_SSG, UpdatedSSG, State),
            Result = ok
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_ADD_NODES, Result),
    {reply, ReplyMessage, UpdatedState};

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_ADD_NODES}}, {{schema_name, SchemaName}, {?DISC_ONLY_COPIES, NodesList}}}}, State) ->

    SSG = maps:get(?STATE_SSG, State),
    
    case mb_ssg:add_schema_disc_only_copies(NodesList, SchemaName, SSG) of 
        {error, Reason} -> 
            UpdatedState = State,
            Result = {error, Reason};

        UpdatedSSG -> 
            UpdatedState = maps:update(?STATE_SSG, UpdatedSSG, State),
            Result = ok
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_ADD_NODES, Result),
    {reply, ReplyMessage, UpdatedState};





%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_DELETE_SERVER_NODE}}, {{schema_name, SchemaName}, {?RAM_COPIES, []}}}}, State) ->

    SSG = maps:get(?STATE_SSG, State),
    
    case mb_ssg:delete_schema_ram_copies([node()], SchemaName, SSG) of 
        {error, Reason} -> 
            UpdatedState = State,
            Result = {error, Reason};

        UpdatedSSG -> 
            UpdatedState = maps:update(?STATE_SSG, UpdatedSSG, State),
            Result = ok
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_DELETE_SERVER_NODE, Result),
    {reply, ReplyMessage, UpdatedState};


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_DELETE_SERVER_NODE}}, {{schema_name, SchemaName}, {?DISC_COPIES, []}}}}, State) ->

    SSG = maps:get(?STATE_SSG, State),
    
    case mb_ssg:delete_schema_disc_copies([node()], SchemaName, SSG) of 
        {error, Reason} -> 
            UpdatedState = State,
            Result = {error, Reason};

        UpdatedSSG -> 
            UpdatedState = maps:update(?STATE_SSG, UpdatedSSG, State),
            Result = ok
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_DELETE_SERVER_NODE, Result),
    {reply, ReplyMessage, UpdatedState};

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_DELETE_SERVER_NODE}}, {{schema_name, SchemaName}, {?DISC_ONLY_COPIES, []}}}}, State) ->

    SSG = maps:get(?STATE_SSG, State),
    
    case mb_ssg:delete_schema_disc_only_copies([node()], SchemaName, SSG) of 
        {error, Reason} -> 
            UpdatedState = State,
            Result = {error, Reason};

        UpdatedSSG -> 
            UpdatedState = maps:update(?STATE_SSG, UpdatedSSG, State),
            Result = ok
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_DELETE_SERVER_NODE, Result),
    {reply, ReplyMessage, UpdatedState};





%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_DELETE_REST_OF_CLUSTER}}, {{schema_name, SchemaName}, {?RAM_COPIES, []}}}}, State) ->

    SSG = maps:get(?STATE_SSG, State),
    
    case mb_ssg:delete_schema_ram_copies(nodes(), SchemaName, SSG) of 
        {error, Reason} -> 
            UpdatedState = State,
            Result = {error, Reason};

        UpdatedSSG -> 
            UpdatedState = maps:update(?STATE_SSG, UpdatedSSG, State),
            Result = ok
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_DELETE_REST_OF_CLUSTER, Result),
    {reply, ReplyMessage, UpdatedState};


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_DELETE_REST_OF_CLUSTER}}, {{schema_name, SchemaName}, {?DISC_COPIES, []}}}}, State) ->

    SSG = maps:get(?STATE_SSG, State),
    
    case mb_ssg:delete_schema_disc_copies(nodes(), SchemaName, SSG) of 
        {error, Reason} -> 
            UpdatedState = State,
            Result = {error, Reason};

        UpdatedSSG -> 
            UpdatedState = maps:update(?STATE_SSG, UpdatedSSG, State),
            Result = ok
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_DELETE_REST_OF_CLUSTER, Result),
    {reply, ReplyMessage, UpdatedState};

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_DELETE_REST_OF_CLUSTER}}, {{schema_name, SchemaName}, {?DISC_ONLY_COPIES, []}}}}, State) ->

    SSG = maps:get(?STATE_SSG, State),
    
    case mb_ssg:delete_schema_disc_only_copies(nodes(), SchemaName, SSG) of 
        {error, Reason} -> 
            UpdatedState = State,
            Result = {error, Reason};

        UpdatedSSG -> 
            UpdatedState = maps:update(?STATE_SSG, UpdatedSSG, State),
            Result = ok
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_DELETE_REST_OF_CLUSTER, Result),
    {reply, ReplyMessage, UpdatedState};






%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_DELETE_NODES}}, {{schema_name, SchemaName}, {?RAM_COPIES, NodesList}}}}, State) ->

    SSG = maps:get(?STATE_SSG, State),
    
    case mb_ssg:delete_schema_ram_copies(NodesList, SchemaName, SSG) of 
        {error, Reason} -> 
            UpdatedState = State,
            Result = {error, Reason};

        UpdatedSSG -> 
            UpdatedState = maps:update(?STATE_SSG, UpdatedSSG, State),
            Result = ok
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_DELETE_NODES, Result),
    {reply, ReplyMessage, UpdatedState};


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_DELETE_NODES}}, {{schema_name, SchemaName}, {?DISC_COPIES, NodesList}}}}, State) ->

    SSG = maps:get(?STATE_SSG, State),
    
    case mb_ssg:delete_schema_disc_copies(NodesList, SchemaName, SSG) of 
        {error, Reason} -> 
            UpdatedState = State,
            Result = {error, Reason};

        UpdatedSSG -> 
            UpdatedState = maps:update(?STATE_SSG, UpdatedSSG, State),
            Result = ok
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_DELETE_NODES, Result),
    {reply, ReplyMessage, UpdatedState};

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_DELETE_NODES}}, {{schema_name, SchemaName}, {?DISC_ONLY_COPIES, NodesList}}}}, State) ->

    SSG = maps:get(?STATE_SSG, State),
    
    case mb_ssg:delete_schema_disc_only_copies(NodesList, SchemaName, SSG) of 
        {error, Reason} -> 
            UpdatedState = State,
            Result = {error, Reason};

        UpdatedSSG -> 
            UpdatedState = maps:update(?STATE_SSG, UpdatedSSG, State),
            Result = ok
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_DELETE_NODES, Result),
    {reply, ReplyMessage, UpdatedState};







%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_SET_SCHEMA_TYPE}}, {{schema_name, SchemaName}, {?SCHEMA_TYPE, SchemaType}}}}, State) ->

    SSG = maps:get(?STATE_SSG, State),
    
    case mb_ssg:set_schema_type(SchemaType, SchemaName, SSG) of 
        {error, Reason} -> 
            UpdatedState = State,
            Result = {error, Reason};

        UpdatedSSG -> 
            UpdatedState = maps:update(?STATE_SSG, UpdatedSSG, State),
            Result = ok
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_ADD_NODES, Result),
    {reply, ReplyMessage, UpdatedState};

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_GET_SCHEMA_ATTRIBUTE}}, {{schema_name, SchemaName}, {schema_attribute, Attribute}}}}, State) ->

    SSG = maps:get(?STATE_SSG, State),
    
    case mb_ssg:get_schema_attribute(Attribute, SchemaName, SSG) of 
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
            Result = {ok, mb_ssg:schema_names(SSG)}
    end,
    
    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_GET_SCHEMA_NAMES, Result),
    {reply, ReplyMessage, State};


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_ADD_FIELD}}, {{schema_name, SchemaName}, {field_name, FieldName}}}}, State) ->

    SSG = maps:get(?STATE_SSG, State),
    
    case mb_ssg:add_field(FieldName, SchemaName, SSG) of 
        {error, Reason} -> 
            UpdatedState = State,
            Result = {error, Reason};

        UpdatedSSG -> 
            UpdatedState = maps:update(?STATE_SSG, UpdatedSSG, State),
            Result = ok 
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_ADD_FIELD, Result),
    {reply, ReplyMessage, UpdatedState};


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_MAKE_FIELD_KEY}}, {{schema_name, SchemaName}, {field_name, FieldName}}}}, State) ->

    SSG = maps:get(?STATE_SSG, State),
    
    case mb_ssg:make_key(FieldName, SchemaName, SSG) of 
        {error, Reason} -> 
            UpdatedState = State,
            Result = {error, Reason};

        UpdatedSSG -> 
            UpdatedState = maps:update(?STATE_SSG, UpdatedSSG, State),
            Result = ok
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_MOVE_FIELD, Result),
    {reply, ReplyMessage, UpdatedState};

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_MOVE_FIELD}}, {{schema_name, SchemaName}, {field_name, FieldName}, {position, ToPosition}}}}, State) ->

    SSG = maps:get(?STATE_SSG, State),
    
    case mb_ssg:move_field(FieldName, ToPosition, SchemaName, SSG) of 
        {error, Reason} -> 
            UpdatedState = State,
            Result = {error, Reason};

        UpdatedSSG -> 
            UpdatedState = maps:update(?STATE_SSG, UpdatedSSG, State),
            Result = ok
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_MOVE_FIELD, Result),
    {reply, ReplyMessage, UpdatedState};


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_SET_FIELD_DESCRIPTION}}, {{schema_name, SchemaName}, {field_name, FieldName}, {?DESCRIPTION, NewDescription}}}}, State) ->

    SSG = maps:get(?STATE_SSG, State),
    
    case mb_ssg:set_field_description(NewDescription, FieldName, SchemaName, SSG) of 
        {error, Reason} -> 
            UpdatedState = State,
            Result = {error, Reason};

        UpdatedSSG -> 
            UpdatedState = maps:update(?STATE_SSG, UpdatedSSG, State),
            Result = ok
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_SET_FIELD_DESCRIPTION, Result),
    {reply, ReplyMessage, UpdatedState};

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_SET_FIELD_LABEL}}, {{schema_name, SchemaName}, {field_name, FieldName}, {?LABEL, NewLabel}}}}, State) ->

    SSG = maps:get(?STATE_SSG, State),
    
    case mb_ssg:set_field_label(NewLabel, FieldName, SchemaName, SSG) of 
        {error, Reason} -> 
            UpdatedState = State,
            Result = {error, Reason};

        UpdatedSSG -> 
            UpdatedState = maps:update(?STATE_SSG, UpdatedSSG, State),
            Result = ok
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_SET_FIELD_LABEL, Result),
    {reply, ReplyMessage, UpdatedState};

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_SET_FIELD_PRIORITY}}, {{schema_name, SchemaName}, {field_name, FieldName}, {?PRIORITY, NewPriority}}}}, State) ->

    SSG = maps:get(?STATE_SSG, State),
    
    case mb_ssg:set_field_priority(NewPriority, FieldName, SchemaName, SSG) of 
        {error, Reason} -> 
            UpdatedState = State,
            Result = {error, Reason};

        UpdatedSSG -> 
            UpdatedState = maps:update(?STATE_SSG, UpdatedSSG, State),
            Result = ok
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_SET_FIELD_PRIORITY, Result),
    {reply, ReplyMessage, UpdatedState};

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_request({?PROT_VERSION, {{{?MSG_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_SET_FIELD_DEFAULT_VALUE}}, {{schema_name, SchemaName}, {field_name, FieldName}, {?DEFAULT_VALUE, NewDefaultValue}}}}, State) ->

    SSG = maps:get(?STATE_SSG, State),
    
    case mb_ssg:set_field_default_value(NewDefaultValue, FieldName, SchemaName, SSG) of 
        {error, Reason} -> 
            UpdatedState = State,
            Result = {error, Reason};

        UpdatedSSG -> 
            UpdatedState = maps:update(?STATE_SSG, UpdatedSSG, State),
            Result = ok
    end,

    ReplyMessage = mb_ipc:build_request_response(SessionId, ?REQUEST_SET_FIELD_DEFAULT_VALUE, Result),
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
    
            case mb_ssg:get_field_attribute(Attribute, FieldName, SchemaName, SSG) of 
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
    
            case mb_ssg:fields(SchemaName, SSG) of 
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
    
            case mb_ssg:get_field(FieldName, SchemaName, SSG) of 
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
    
            case mb_ssg:field_count(SchemaName, SSG) of 
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
    
            case mb_ssg:mandatory_field_count(SchemaName, SSG) of 
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
    
            case mb_ssg:field_names(SchemaName, SSG) of 
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
    
            case mb_ssg:field_position(FieldName, SchemaName, SSG) of 
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

            case mb_ssg:read(Key, SchemaName, SSG) of 
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
    
            case mb_ssg:select(SchemaName, Field, Oper, Value, SSG) of 
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
    
            case mb_ssg:select_or(SchemaName, Field, Oper1, Value1, Oper2, Value2, SSG) of 
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
    
            case mb_ssg:select_and(SchemaName, Field, Oper1, Value1, Oper2, Value2, SSG) of 
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
    
            case mb_ssg:add(Record, SSG) of 
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
    
            case mb_ssg:add(SchemaName, Record, SSG) of 
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
    
            case mb_ssg:add(SchemaName, Key, Data, SSG) of 
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
            case code:module_status(Module) of
                not_loaded -> {error, module_not_loaded};
                _ ->  {ok, Module}
            end;
        false -> false
    end.


%-------------------------------------------------------------
%-------------------------------------------------------------
load_module(State) ->
    
    case maps:get(?STATE_MODULE, State) of
        [] -> {error, module_name_not_set};
        Module ->
            case code:soft_purge(Module) of 
                true ->
                    case code:load_file(Module) of 
                        {module, Module} -> Module;
                        {error, Reason} -> {error, Reason}
                    end;
                false -> {error, {cannot_purge_module, Module}}
            end
    end.


%-------------------------------------------------------------
%-------------------------------------------------------------
update_state_new_ssg(State, NewSSG) -> 
    UpdatedStateInterim1 = maps:update(?STATE_SSG, NewSSG, State),
    % The module name is always the SSG name
    UpdatedStateInterim2 = maps:update(?STATE_MODULE, maps:get(?NAME, NewSSG), UpdatedStateInterim1), 
    maps:update(?STATE_USE_MODULE, false, UpdatedStateInterim2).

%-------------------------------------------------------------
%-------------------------------------------------------------
get_client_directories() -> {filename:absname(?AUTO_GEN_CLIENT_SRC_DIR),
                             filename:absname(?AUTO_GEN_CLIENT_INCLUDE_DIR),
                             filename:absname(?AUTO_GEN_CLIENT_EBIN_DIR)}.