-module(schema_modeller_server).
-behaviour(gen_server).

-export([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-include("../include/db_access_ipc.hrl").

% Public API
start_link() ->
    gen_server:start_link({local, schema_modeller}, ?MODULE, [], []).

% Protocol Structure
%
%   {Version, {MessageType, Data}}
%
%    MessageType = {request|?MSG_TYPE_REQUEST_RESPONSE, MessageName}
%    Data = [{Key, Value}]
%

init([]) ->
    {ok, #{}}.

handle_call({?PROT_VERSION, {{request, new_model}, []}}, _From, State) -> 
    {reply, {?PROT_VERSION, {{?MSG_TYPE_REQUEST_RESPONSE, new_model}, {result, ok}, [{model_handle, 1001}]}}, State};

handle_call({?PROT_VERSION, {request, delete_model}}, _From, State) -> {reply, ok, State};

handle_call({model_schema, Schema}, _From, State) ->
    % Example: Process schema modelling request
    io:format("Schema modelled: ~p~n", [Schema]),
    {reply, ok, State};
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
