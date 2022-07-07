%%% @doc Manage a connection to AMQP
%% This module provides a persistent connection to AMQP for an Erlang node(). It will
%% make sure a connection is kept alive toward the target, and it will try reconnecting
%% if the connection is lost (and this process is supervised).
%%
%% Once a connection is established, this process will register itself in gproc under the
%% name `{turtle, connection, Name}', where `Name' is given when start_link/1'ing this
%% process.
%%% @end
%% @private
-module(turtle_conn).
-behaviour(gen_server).
-include_lib("amqp_client/include/amqp_client.hrl").

%% Lifetime
-export([
	start_link/2
]).

%% API
-export([
	conn/1,
	close/1,
    new/1
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    stop/1,
    terminate/2,
    code_change/3
]).

-define(DEFAULT_RETRY_TIME, 15*1000).
-define(DEFAULT_ATTEMPT_COUNT, 10).

-type network_connection() :: {string(), inet:port_number()}.

-record(conn_group, {
    orig :: [{atom(), [network_connection()]}],
    orig_group = [] :: [network_connection()],
    attempts = ?DEFAULT_ATTEMPT_COUNT :: non_neg_integer(),
    next
}).

-record(state, {
          name :: atom(),
          network_params :: #amqp_params_network{},
          cg :: #conn_group{},
          connection = undefined :: undefined | pid(),
          monitor = undefined :: undefined | reference(),
          deadline :: undefined | reference(),
          retry_time = ?DEFAULT_RETRY_TIME :: pos_integer()
}).


%% LIFETIME MAINTENANCE
%% ----------------------------------------------------------
start_link(Name, Configuration) ->
    gen_server:start_link(?MODULE, [Name, Configuration], []).
	
close(Name) ->
    call(Name, close).

conn(Name) ->
    call(Name, conn).
    
call(Loc, Msg) ->
    case gproc:where({n,l,{turtle, connection, Loc}}) of
        undefined ->
            {error, undefined};
        Pid when is_pid(Pid) ->
            gen_server:call(Pid, Msg, 20*1000)
    end.

new(Configuration) ->
    turtle_sup:add_connection(Configuration).

stop(Name) ->
    turtle_sup:stop_connection(Name).

%% CALLBACKS
%% -------------------------------------------------------------------

%% @private
init([Name, Configuration]) ->
    self() ! connect,
    TRef = init_deadline(Configuration),
    {ok, #state {
            name = Name,
            cg = group_init(Configuration),
            deadline = TRef,
            network_params = turtle_config:conn_params(Configuration)
    }}.

%% @private
handle_call(_Msg, _From, #state { connection = undefined } = State) ->
    {reply, {error, no_amqp_connection}, State};
handle_call(close, _From, State) ->
    {stop, normal, ok, State};
handle_call(conn, _From, #state { connection = undefined } = State) ->
    {reply, {error, no_amqp_connection}, State};
handle_call(conn, _From, #state { connection = Conn } = State) ->
    {reply, Conn, State};
handle_call(Call, From, State) ->
    logger:warning("Unknown call from ~p: ~p", [From, Call]),
    {reply, {error, unknown_call}, State}.

%% @private
handle_cast(Cast, State) ->
    logger:warning("Unknown cast: ~p", [Cast]),
    {noreply, State}.

%% @private
handle_info({connection_closed, _Conn, Reason}, State) ->
    ExitReason = case Reason of
                     normal -> normal;
                     shutdown -> normal;
                     {shutdown, _} -> normal;
                     Otherwise -> {amqp_connection_died, Otherwise}
                 end,
    {stop, ExitReason, State};
handle_info(connect, #state { name = Name,
                              deadline = TRef,
                              retry_time = Retry } = State) ->
    case connect(State) of
        {ok, ConnectedState} ->
            cancel_deadline(TRef),
            reg(Name),
            {noreply, ConnectedState};
        {error, unknown_host, #state { cg = CG } = NextState} ->
            logger:error("Unknown host while connecting to RabbitMQ: ~p",
                [group_report(CG)]),
            {stop, {error, unknown_host}, NextState};
        {error,econnrefused, #state { cg = CG } = NextState} ->
            logger:info("AMQP Connection refused, retrying in ~Bs: ~p",
                [Retry div 1000, group_report(CG)]),
            erlang:send_after(Retry, self(), connect),
            {noreply, NextState};
        {error, timeout, #state { cg = CG } = NextState} ->
            logger:warning("Timeout while connecting to RabbitMQ, retrying in ~Bs: ~p",
                [Retry div 1000, group_report(CG)]),
            erlang:send_after(Retry, self(), connect),
            {noreply, NextState};
        {error, Reason, #state { cg = CG } = NextState} ->
            logger:warning("Error connecting to RabbitMQ, reason: ~p, retrying in ~Bs: ~p",
                [Reason, Retry div 1000, group_report(CG)]),
            erlang:send_after(Retry, self(), connect),
            {noreply, NextState}
    end;
handle_info({timeout, TRef, deadline}, #state { deadline = TRef } = State) ->
    {stop, deadline, State};
handle_info({'DOWN', MRef, process, _Pid, _Reason}, #state { monitor = MRef } = State) ->
    {stop, connection_died, State};
handle_info(Info, State) ->
    logger:warning("Received unknown info-message in turtle_conn: ~p", [Info]),
    {noreply, State}.

%% @private
terminate(_Reason, _State) ->
    ok.

%% @private
code_change(_, State, _) ->
    {ok, State}.

%%
%% INTERNAL FUNCTIONS
%%
connect(#state { network_params = NP, cg = CG } = State) ->
    {ok, _N, {Host, Port}, CG2} = group_next(CG),
    Network = NP#amqp_params_network {
    	host = Host,
    	port = Port
    },
    case turtle:open_connection(Network) of
       {ok, Conn} ->
            MRef = erlang:monitor(process, Conn),
            {ok, State#state { monitor = MRef,
                               connection = Conn, cg = CG2 }};
       {error, Reason} -> {error, Reason, State#state { cg = CG2} }
    end.

reg(Name) ->
    true = gproc:reg({n,l, {turtle, connection, Name}}).

%%
%% Connection retry handling
%%
group_next(#conn_group { orig = [] }) ->
    {error, no_connection_groups_defined};
group_next(#conn_group { orig = [{_N, G} | _] = Orig, next = [] } = CG) ->
    group_next(CG#conn_group {
        orig_group = G,
        attempts = ?DEFAULT_ATTEMPT_COUNT,
        next = Orig });
group_next(#conn_group { orig_group = G, next = [{N, []} | Cns] } = CG) ->
    group_next(CG#conn_group { next = [{N, G} | Cns] });
group_next(#conn_group { attempts = 0, next = Next } = CG) ->
    logger:warning("Turtle AMQP Connect: Exhausted every host in group, moving to next group"),
    case Next of
        [_] ->
            logger:critical(
              "Turtle AMQP Connect: "
              "Exhausted all connection groups, starting over with the first group"),
            group_next(CG#conn_group { next = [] });
        [_, {N, G} | Ns] ->
            group_next(CG#conn_group {
                orig_group = G,
                attempts = ?DEFAULT_ATTEMPT_COUNT,
                next = [{N, G} | Ns] })
    end;
group_next(#conn_group { attempts = A, next = [{N, [C|Cs]} | Ns] } = CG) ->
    {ok, N, C, CG#conn_group { attempts = A - 1, next = [{N, Cs} | Ns] }}.

group_report(#conn_group { attempts = A, next = [{Nm, _} | _] = Next }) ->
    [{cursor, Nm}, {attempts, A}, {can_continue, [N || {N, _} <- Next] }].

group_init(#{ connections := Cs }) ->
    #conn_group {
        orig = canonicalize_connections(Cs),
        attempts = ?DEFAULT_ATTEMPT_COUNT,
        next = [] }.

init_deadline(#{ deadline := Ms }) ->
    erlang:start_timer(Ms, self(), deadline);
init_deadline(#{}) ->
    undefined.

cancel_deadline(undefined) -> ok;
cancel_deadline(TRef) -> erlang:cancel_timer(TRef).

canonicalize_connections(Cs) ->
    C = fun
        ({Host, Port}) when is_integer(Port) -> {Host, Port};
        ({Host, Port}) when is_list(Port) -> {Host, list_to_integer(Port)}
    end,
    F = fun({Name, HPs}) -> {Name, [C(HP) || HP <- HPs]} end,
    [F(Part) || Part <- Cs].
