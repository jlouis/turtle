%%% @doc Manage an AMQP channel
%%% @end
%% @private
-module(turtle_channel).
-behaviour(gen_server).
-include_lib("amqp_client/include/amqp_client.hrl").

%% Lifetime
-export([
    start_link/1
]).

%% API
-export([
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-record(state, {
    name,
    channel,
    conf,
    conn_ref
 }).

%% LIFETIME MAINTENANCE
%% ----------------------------------------------------------
start_link(Configuration) ->
    gen_server:start_link(?MODULE, [Configuration], []).
	
%% CALLBACKS
%% -------------------------------------------------------------------

%% @private
init([#{ connection := ConnName } = Conf]) ->
    ok = validate_config(Conf),
    Ref = gproc:nb_wait({n,l,{turtle, connection, ConnName}}),
    {ok, {initializing, Ref, Conf}}.

%% @private
handle_call(Call, From, State) ->
    lager:warning("Unknown call from ~p: ~p", [From, Call]),
    {reply, {error, unknown_call}, State}.

%% @private
handle_cast(Cast, State) ->
    lager:warning("Unknown cast: ~p", [Cast]),
    {noreply, State}.

%% @private
handle_info({gproc, Ref, registered, {_, Pid, _}}, {initializing, Ref,
    #{
      name := Name,
      connection := ConnName,
      declarations := Decls,
      function := _Fun,
      consume_queue := _Queue,
      subscriber_count := K
     } = Conf }) ->
    {ok, Ch} = turtle:open_channel(ConnName),
    ok = setup_qos(Ch, Conf),
    ok = amqp_channel:register_return_handler(Ch, self()),
    ok = turtle:declare(Ch, Decls),
    Pool = gproc:where({n,l,{turtle,service_pool, Name}}),
    add_subscribers(Pool, Conf#{ channel => Ch}, K),
    MRef = erlang:monitor(process, Pid),
    reg(Name),
    {noreply, #state { conn_ref = MRef, channel = Ch, conf = Conf, name = Name }};
handle_info({'DOWN', MRef, process, _, Reason}, #state { conn_ref = MRef } = State) ->
    {stop, {error, {connection_down, Reason}}, State};
handle_info(#'basic.return' {} = Return, #state { name = Name } = State) ->
    lager:info("Channel ~p received a return from AMQP: ~p", [Name, Return]),
    {noreply, State};
handle_info(Info, State) ->
    lager:warning("Unknown info msg: ~p", [Info]),
    {noreply, State}.

%% @private
terminate(_Reason, #state { channel = Ch }) when is_pid(Ch) ->
    ok = amqp_channel:unregister_return_handler(Ch),
    ok;
terminate(_Reason, _State) ->
    ok.

%% @private
code_change(_, State, _) ->
    {ok, State}.

%%
%% INTERNAL FUNCTIONS
%%

add_subscribers(_Pool, _Conf, 0) -> ok;
add_subscribers(Pool, Conf, K) ->
    turtle_subscriber_pool:add_subscriber(Pool, Conf),
    add_subscribers(Pool, Conf, K-1).

%% Make sure our config object is inhabitated correctly.
validate_config(#{
    connection := _Conn,
    name := _N,
    declarations := _Ds,
    function := _Fun,
    consume_queue := _Q,
    subscriber_count := _K }) -> ok.

reg(N) ->
    true = gproc:reg({n,l,{turtle,service_channel,N}}).

setup_qos(Ch, #{ prefetch_count := K }) ->
    #'basic.qos_ok'{} = amqp_channel:call(Ch, #'basic.qos' { prefetch_count = K }),
    ok;
setup_qos(_Ch, _Conf) -> ok.

