-module(db_server).
-behaviour(gen_server).

-include("../include/db_ipc.hrl").

-export([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
start_link() ->
    gen_server:start_link({global, ?MODULE}, ?MODULE, [], []).

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
init([]) -> 
    process_flag(trap_exit, true),
    {ok, #{sessions=>[]}}.


%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
handle_call({?PROT_VERSION, {{{session_id, {0,0,0}}, {?MSG_TYPE_REQUEST, ?REQUEST_CONNECT}}, {}}}, ReceivedClientPid, State) ->

    % Strip out the alias if it is there
    ClientPid = db_ipc:remove_pid_alias(ReceivedClientPid),

    io:format("[db_access::server::~p]: connect request from: ~p~n", [self(), ClientPid]),

    % Generate a random token to allow client and the worker
    % validate one another (not encrypted, very basic security)
    Token = random_10_digit_number(),

    % Launch a worker to handle the new session
    {ok, WorkerPid} = db_worker:start(self(), ClientPid, Token),

    SessionId = {WorkerPid, ClientPid, Token},

    SessionsList = maps:get(sessions, State),
    UpdatedSessionsList = [SessionId | SessionsList],

    UpdatedState = maps:update(sessions, UpdatedSessionsList, State),
    Message = db_ipc:build_connect_response(SessionId),
    
    {reply, Message, UpdatedState};

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
handle_call({?PROT_VERSION, {{{session_id, {0,0,0}}, {?MSG_TYPE_COMMAND, ?COMMAND_GET_SESSIONS}}, {}}}, _ClientPid, State) ->

    SessionsList = maps:get(sessions, State),
    Message = db_ipc:build_command_response(?COMMAND_GET_SESSIONS, SessionsList),
    
    {reply, Message, State};

%-------------------------------------------------------------
%
%-------------------------------------------------------------
handle_call(Request, From, State) ->

    % Strip out the alias if it is there
    ClientPid = db_ipc:remove_pid_alias(From),

    io:format("[db_access::server::~p]: unsupported ~p: ~p from ~p~n", [self(), ?MSG_TYPE_REQUEST, Request, ClientPid]),

    case db_ipc:get_session_id(Request) of 
        {error, _Error} ->
            Message = db_ipc:build_error(invalid_message_format);
        
        SessionId -> 

            % We send back an error for message type since we do not recognize
            % the incoming message and cannot send back a ?MSG_TYPE_REQUEST_RESPONSE to it.
            Message = db_ipc:build_error(SessionId, not_supported)
    end,
            

    {reply, Message, State}.

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
handle_cast(Message, State) ->
    io:format("[db_access::server::~p]: unsupported message: ~p~n", [self(), Message]),
    {noreply, State}.


%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
handle_info({'EXIT', WorkerPid, Reason}, State) ->
    io:format("[db_access::server::~p]: worker ~p exited, reason: ~p~n", [self(), WorkerPid, Reason]),

    SessionList = maps:get(sessions, State),
    UpdatedSessionList = lists:keydelete(WorkerPid, 1, SessionList),
    UpdatedState = maps:update(sessions, UpdatedSessionList, State),
    {noreply, UpdatedState};


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_info(Info, State) ->
    io:format("[db_access::server::~p]: unsupported info notification: ~p~n", [self(), Info]),
    {noreply, State}.

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
terminate(Reason, _State) ->
    io:format("[db_access::server::~p]: terminate: ~p~n", [self(), Reason]),
    ok.

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
