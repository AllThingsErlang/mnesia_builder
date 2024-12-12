-module(db_access_api).
-include("../include/db_access_ipc.hrl").

-export([connect/0,
         get_sessions/0]).



%-------------------------------------------------------------
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
connect() ->

    io:format("[db_access::api::~p]: connect ...~n", [self()]),

    ConnectMessage = db_access_ipc:build_request_connect(),
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
                    
                    StartMessage = db_access_ipc:build_request_start_session(NewSessionId),
                
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
% Function: 
% Purpose:  
% Returns: 
%-------------------------------------------------------------
get_sessions() -> 

    io:format("[db_access::api::~p]: get_sessions ...~n", [self()]),

    GetSessionsMessage = db_access_ipc:build_command_get_sessions(),
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



                
