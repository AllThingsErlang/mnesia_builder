-module(db_access_worker).
-behaviour(gen_server).

-include("../include/db_access_ipc.hrl").

-export([start/3, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(STATE_SERVER, server).
-define(STATE_SESSION_ID, session_id).
-define(STATE_SESSION_ACTIVE, session_active).
-define(STATE_TIMER, timer).
-define(STATE_SPECIFICATIONS, specifications).

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
start(ServerPid, ClientPid, Token) ->
    gen_server:start_link(?MODULE, {ServerPid, ClientPid, Token}, []).

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
init({ServerPid, ClientPid, Token}) ->
    io:format("[db_access::worker::~p]: serving client ~p with token ~p~n", [self(), ClientPid, Token]),
    process_flag(trap_exit, true),
    TimerRef = erlang:start_timer(3000, self(), session_start_timer),
    {ok, #{?STATE_SERVER=>ServerPid, ?STATE_SESSION_ID=>{self(),ClientPid,Token}, ?STATE_SESSION_ACTIVE=>false, ?STATE_TIMER=>TimerRef, ?STATE_SPECIFICATIONS=>#{}}}.


%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
handle_call({?PROT_VERSION, {{{?STATE_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_START_SESSION}}, {}}}, From, State) ->

    io:format("[db_access::worker::~p]: start_session from ~p~n", [self(), From]),

    {SessionWorkerPid, SessionClientPid, SessionToken} = maps:get(?STATE_SESSION_ID, State),

    case SessionId of 

        {SessionWorkerPid, SessionClientPid, SessionToken} when (element(1, From) == element(1, SessionClientPid)) ->

                io:format("[db_access::worker::~p]: start_session accepted, cancelling timer~n", [self()]),

                ReplyMessage = db_access_ipc:build_start_session_response(SessionId),
                
                TimerRef = maps:get(?STATE_TIMER, State),
                case TimerRef of 
                    [] -> ok;
                    _ -> erlang:cancel_timer(TimerRef)
                end,

                UpdatedState = maps:update(?STATE_TIMER, [], maps:update(?STATE_SESSION_ACTIVE, true, State)),

                {reply, ReplyMessage, UpdatedState};
                
        Other -> 
            
            io:format("[db_access::worker::~p]: start_session rejected: ~p~n", [self(), Other]),
            io:format("[db_access::worker::~p]: From: ~p~n", [self(), From]),
            io:format("[db_access::worker::~p]: SessionClientPid: ~p~n", [self(), SessionClientPid]),


            ReplyMessage = db_access_ipc:build_start_session_response(SessionId, invalid_session_credentials),
            
            {reply, ReplyMessage, State}
    end;

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_call({?PROT_VERSION, {{{?STATE_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, end_session}}, {}}}, From, State) ->

    io:format("[db_access::worker::~p]: end_session from ~p~n", [self(), From]),

    {SessionWorkerPid, SessionClientPid, SessionToken} = maps:get(?STATE_SESSION_ID, State),

    case SessionId of 
        {SessionWorkerPid, SessionClientPid, SessionToken} when (From == SessionClientPid) -> 
            io:format("[db_access::worker::~p]: end_session accepted~n", [self()]),

            UpdatedState = maps:update(?STATE_SESSION_ACTIVE, false, State),
            {stop, normal, ok, UpdatedState};

         Other -> 
            io:format("[db_access::worker::~p]: end_session rejected: ~p~n", [self(), Other]),
            ReplyMessage = db_access_ipc:build_end_session_response(SessionId, invalid_session_credentials),
            {reply, ReplyMessage, State}
    end;

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_call({?PROT_VERSION, {{{?STATE_SESSION_ID, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_NEW_SPECIFICATIONS}}, {}}}, From, State) ->

    io:format("[db_access::worker::~p]: ~p from ~p~n", [self(), ?REQUEST_NEW_SPECIFICATIONS, From]),

    {SessionWorkerPid, SessionClientPid, SessionToken} = maps:get(?STATE_SESSION_ID, State),

    case SessionId of 
        {SessionWorkerPid, SessionClientPid, SessionToken} when (From == SessionClientPid) -> 
            io:format("[db_access::worker::~p]: ~p accepted~n", [self(), ?REQUEST_NEW_SPECIFICATIONS]),

            UpdatedState = maps:update(?STATE_SPECIFICATIONS, maps:new(), State),
            ReplyMessage = db_access_ipc:build_request_response(SessionId, ?REQUEST_NEW_SPECIFICATIONS, ok),
            {reply, ReplyMessage, UpdatedState};

         _ -> 
            io:format("[db_access::worker::~p]: ~p rejected: ~p~n", [self(), ?REQUEST_NEW_SPECIFICATIONS, invalid_session_credentials]),
            ReplyMessage = db_access_ipc:build_request_response(SessionId, invalid_session_credentials),
            {reply, ReplyMessage, State}
    end;

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
handle_call(Request, _From, State) ->
    io:format("[db_access::worker::~p]: unsupported request: ~p~n", [self(), Request]),

    ReplyMessage = db_access_ipc:build_error(unknown_request),
     {reply, ReplyMessage, State}.

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
handle_cast(Message, State) ->
    io:format("[db_access::worker::~p]: unsupported message: ~p~n", [self(), Message]),
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
                    io:format("[db_access::worker::~p]: unknown timer reference ~p~n", [self(), IncomingTimerRef]),
                    {noreply, State}
            end 
    end;

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
handle_info({'EXIT', _FromPid, Reason}, State) ->
    io:format("[db_access::worker::~p]: received exit signal, reason: ~p~n", [self(), Reason]),
    {stop, Reason, State};

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_info({'DOWN', _Ref, process, ClientPid, _Reason}, State) ->
    io:format("[db_access::worker::~p]: client disconnected~n", [self()]),

    % Handle client disconnection
    SessionId = maps:get(?STATE_SESSION_ID, State),

    case SessionId of
        {_, ClientPid, _} ->
            io:format("[db_access::worker::~p]:  client ~p disconnected. Stopping worker.~n", [self(), ClientPid]),
            {stop, normal, State};
        _ ->
            io:format("[db_access::worker::~p]:  no associated session id found: ~p.~n", [self(), SessionId]),
            {noreply, State}
    end;

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_info(Info, State) ->
    io:format("[db_access::worker::~p]: unsupported info notification: ~p~n", [self(), Info]),
    {noreply, State}.

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
terminate(_Reason, State) ->
    io:format("[db_access::worker::~p]: terminated for session ~p~n", [self(), maps:get(?STATE_SESSION_ID, State)]),
    ok.

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
