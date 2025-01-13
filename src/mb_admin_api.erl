-module(mb_admin_api).
-include("../include/mb_ipc.hrl").
-include("../include/mb_api.hrl").
-include("../include/mb.hrl").


-export([get_sessions/0, restart/0]).

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

                {error, Reason} -> 
                        io:format("[db::api::~p]: get_sessions aborted: ~p~n", [self(), Reason]),
                        {error, Reason}
            end
    end.
    


%-------------------------------------------------------------
% 
%-------------------------------------------------------------
-spec restart() -> ok.
%-------------------------------------------------------------
restart() -> 

    io:format("[db::admin_api::~p]: restart(...)~n", [self()]),

    Message = mb_ipc:build_command(?COMMAND_RESTART),
    ServerPid = global:whereis_name(?SERVER_DB),

    case ServerPid of 

        undefined -> 
            io:format("[db::admin_api::~p]: server  ~p is not running~n", [self(), ?SERVER_DB]),
            {error, server_not_running};

        _ ->

            case mb_ipc:call(ServerPid, Message) of

                {?PROT_VERSION, {{{session_id, {0, 0, 0}}, {?MSG_TYPE_COMMAND_RESPONSE, ?COMMAND_RESTART}}, {result, ok}}} ->
                    io:format("[db::admin_api::~p]: server restarting: ~n", [self()]),
                    ok;

                {?PROT_VERSION, {{{session_id, {0, 0, 0}}, {?MSG_TYPE_COMMAND_RESPONSE, ?COMMAND_RESTART}}, {{result, {error, Reason}}}}} ->
                    io:format("[db::admin_api::~p]: restart rejected: ~p~n", [self(), Reason]),
                    {error, Reason};

                {error, Reason} -> 
                        io:format("[db::admin_api::~p]: restart aborted: ~p~n", [self(), Reason]),
                        {error, Reason}
            end
    end.