-module(mb_server).
-behaviour(gen_server).

-include("../include/mb_ipc.hrl").

-export([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).


-define(STATE_SESSIONS, sessions).
-define(STATE_SESSION_DIR, session_dir).

-define(DIR_SESSIONS, "sessions").

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
start_link() ->
    io:format("[mb::server::~p]: start_link started~n", [self()]),
    Result = gen_server:start_link({global, ?MODULE}, ?MODULE, [], []),
    io:format("[mb::server::~p]: start_link returned ~p~n", [self(), Result]),
    Result.

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
init([]) -> 
    process_flag(trap_exit, true),

    % Remove any lingering worker pids.
    mb_tables:unassign_all_workers(),

    State = #{?STATE_SESSIONS=>[], ?STATE_SESSION_DIR=>?DIR_SESSIONS},

    io:format("[mb::server::~p]: linked processes: ~p~n", [self(), mb_utilities:get_linked_processes()]),

    case file:make_dir(?DIR_SESSIONS) of
        ok -> 
            %io:format("[mb::server::~p]: session dir created, init ok~n", [self()]),
            {ok, State};

        {error, eexist} -> 
            %io:format("[mb::server::~p]: session dir available, init ok~n", [self()]),
            {ok, State};

        {error, Reason} -> 
            %io:format("[mb::server::~p]: could not create session dir~n", [self()]),
            {stop, {error, {failed_to_create_session_root_dir, Reason}}}
    end.



%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
handle_call({?PROT_VERSION, {{{session_id, {0,0,0}}, {?MSG_TYPE_REQUEST, ?REQUEST_CONNECT}}, {}}}, ReceivedClientPid, State) ->

    % Strip out the alias if it is there
    ClientPid = mb_ipc:remove_pid_alias(ReceivedClientPid),

    io:format("[mb::server::~p]: connect request from: ~p~n", [self(), ClientPid]),

    % Generate a random token to allow client and the worker
    % validate one another (not encrypted, very basic security)
    Token = random_10_digit_number(),

    % Launch a worker to handle the new session
    {ok, WorkerPid} = mb_worker:start(self(), ClientPid, Token, maps:get(?STATE_SESSION_DIR, State)),

    SessionId = {WorkerPid, ClientPid, Token},

    SessionsList = maps:get(sessions, State),
    UpdatedSessionsList = [SessionId | SessionsList],

    UpdatedState = maps:update(sessions, UpdatedSessionsList, State),
    Message = mb_ipc:build_connect_response(SessionId),
    
    {reply, Message, UpdatedState};

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
handle_call({?PROT_VERSION, {{{session_id, {0,0,0}}, {?MSG_TYPE_COMMAND, ?COMMAND_GET_SESSIONS}}, {}}}, _ClientPid, State) ->

    SessionsList = maps:get(sessions, State),
    Message = mb_ipc:build_command_response(?COMMAND_GET_SESSIONS, {ok, SessionsList}),
    
    {reply, Message, State};

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
handle_call({?PROT_VERSION, {{{session_id, {0,0,0}}, {?MSG_TYPE_COMMAND, ?COMMAND_TEST_CRASH}}, {}}}, _ClientPid, State) ->

    Message = 0/0,
    {reply, Message, State};

%-------------------------------------------------------------
%
%-------------------------------------------------------------
handle_call(Request, From, State) ->

    % Strip out the alias if it is there
    ClientPid = mb_ipc:remove_pid_alias(From),

    io:format("[mb::server::~p]: unsupported ~p: ~p from ~p~n", [self(), ?MSG_TYPE_REQUEST, Request, ClientPid]),

    case mb_ipc:get_session_id(Request) of 
        {error, _Error} ->
            Message = mb_ipc:build_error(invalid_message_format);
        
        SessionId -> 

            % We send back an error for message type since we do not recognize
            % the incoming message and cannot send back a ?MSG_TYPE_REQUEST_RESPONSE to it.
            Message = mb_ipc:build_error(SessionId, not_supported)
    end,
            

    {reply, Message, State}.

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
handle_cast(Message, State) ->
    io:format("[mb::server::~p]: unsupported message: ~p~n", [self(), Message]),
    {noreply, State}.


%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
handle_info({'EXIT', WorkerPid, Reason}, State) ->
    io:format("[mb::server::~p]: worker ~p exited, reason: ~p~n", [self(), WorkerPid, Reason]),

    SessionList = maps:get(sessions, State),
    UpdatedSessionList = lists:keydelete(WorkerPid, 1, SessionList),
    UpdatedState = maps:update(sessions, UpdatedSessionList, State),
    {noreply, UpdatedState};


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_info(Info, State) ->
    io:format("[mb::server::~p]: unsupported info notification: ~p~n", [self(), Info]),
    {noreply, State}.

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
terminate(Reason, State) ->
    io:format("[mb::server::~p]: terminate: ~p~n", [self(), Reason]),
    shutdown_workers(maps:get(?STATE_SESSIONS, State)).

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
random_10_digit_number() ->
    % Ensure rand:uniform/1 generates a number in the range [1000000000, 9999999999]
    Min = 1000000000,
    Max = 9999999999,
    Min + rand:uniform(Max - Min + 1) - 1.

%-------------------------------------------------------------
%-------------------------------------------------------------
shutdown_workers([]) -> ok;
shutdown_workers([{WorkerPid, _, _} | T]) -> 
    gen_server:cast(WorkerPid, shutdown),
    shutdown_workers(T).
