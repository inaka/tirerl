-module(tirerl_worker).

-behaviour(gen_server).

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {worker :: pid()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init(Args) ->
    process_flag(trap_exit, true),
    {ok, Worker} = tirerl:start_link(Args),
    {ok, #state{worker=Worker}}.

handle_call({F, A1, A2, A3, A4, A5, A6}, _From, State=#state{worker=Worker}) ->
    Reply = tirerl:F(Worker, A1, A2, A3, A4, A5, A6),
    {reply, Reply, State};
handle_call({F, A1, A2, A3, A4, A5}, _From, State=#state{worker=Worker}) ->
    Reply = tirerl:F(Worker, A1, A2, A3, A4, A5),
    {reply, Reply, State};
handle_call({F, A1, A2, A3, A4}, _From, State=#state{worker=Worker}) ->
    Reply = tirerl:F(Worker, A1, A2, A3, A4),
    {reply, Reply, State};
handle_call({F, A1, A2, A3}, _From, State=#state{worker=Worker}) ->
    Reply = tirerl:F(Worker, A1, A2, A3),
    {reply, Reply, State};
handle_call({F, A1, A2}, _From, State=#state{worker=Worker}) ->
    Reply = tirerl:F(Worker, A1, A2),
    {reply, Reply, State};
handle_call({F, A1}, _From, State=#state{worker=Worker}) ->
    Reply = tirerl:F(Worker, A1),
    {reply, Reply, State};
handle_call({F}, _From, State=#state{worker=Worker}) ->
    Reply = tirerl:F(Worker),
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {stop, unhandled_cast, State}.

handle_info({'EXIT', _, shutdown}, State) ->
    {noreply, State};

handle_info(_Info, State) ->
    {stop, unhandled_info, State}.

terminate(_Reason, #state{worker=Worker}) ->
    ok = tirerl:stop(Worker),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
