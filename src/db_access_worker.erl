-module(db_access_worker).
-behaviour(gen_server).

-include("../include/db_access_ipc.hrl").

-export([start/3, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

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
    {ok, #{server=>ServerPid, session_id=>{self(),ClientPid,Token}, session_active=>false, timer=>TimerRef}}.


%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
handle_call({?PROT_VERSION, {{{session_id, SessionId}, {?MSG_TYPE_REQUEST, ?REQUEST_START_SESSION}}, {}}}, From, State) ->

    io:format("[db_access::worker::~p]: start_session from ~p~n", [self(), From]),

    {SessionWorkerPid, SessionClientPid, SessionToken} = maps:get(session_id, State),

    case SessionId of 

        {SessionWorkerPid, SessionClientPid, SessionToken} when (element(1, From) == element(1, SessionClientPid)) ->

                io:format("[db_access::worker::~p]: start_session accepted, cancelling timer~n", [self()]),

                ReplyMessage = db_access_ipc:build_response_start_session(SessionId),
                
                TimerRef = maps:get(timer, State),
                case TimerRef of 
                    [] -> ok;
                    _ -> erlang:cancel_timer(TimerRef)
                end,

                UpdatedState = maps:update(timer, [], maps:update(session_active, true, State)),

                {reply, ReplyMessage, UpdatedState};
                
        Other -> 
            
            io:format("[db_access::worker::~p]: start_session rejected: ~p~n", [self(), Other]),
            io:format("[db_access::worker::~p]: From: ~p~n", [self(), From]),
            io:format("[db_access::worker::~p]: SessionClientPid: ~p~n", [self(), SessionClientPid]),


            ReplyMessage = db_access_ipc:build_response_start_session(SessionId, invalid_session_credentials),
            
            {reply, ReplyMessage, State}
    end;

%-------------------------------------------------------------
% 
%-------------------------------------------------------------
handle_call({?PROT_VERSION, {{{session_id, SessionId}, {?MSG_TYPE_REQUEST, end_session}}, {}}}, From, State) ->

    io:format("[db_access::worker::~p]: end_session from ~p~n", [self(), From]),

    {SessionWorkerPid, SessionClientPid, SessionToken} = maps:get(session_id, State),

    case SessionId of 
        {SessionWorkerPid, SessionClientPid, SessionToken} when (From == SessionClientPid) -> 
            io:format("[db_access::worker::~p]: end_session accepted~n", [self()]),

            UpdatedState = maps:update(session_active, false, State),
            {stop, normal, ok, UpdatedState};

         Other -> 
            io:format("[db_access::worker::~p]: end_session rejected: ~p~n", [self(), Other]),
            ReplyMessage = db_access_ipc:build_response_end_session(SessionId, invalid_session_credentials),
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

    case maps:get(session_active, State) of 
        true -> 
            % Ignore
            UpdatedState = maps:update(timer, [], State),
            {noreply, UpdatedState};

        false -> 
            TimerRef = maps:get(timer, State),

            case (IncomingTimerRef == TimerRef) of 
                true -> 
                    UpdatedState = maps:update(session_active, false, State),
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
    SessionId = maps:get(session_id, State),

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
    io:format("[db_access::worker::~p]: terminated for session ~p~n", [self(), maps:get(session_id, State)]),
    ok.

%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
