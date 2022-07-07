%%% @doc Manage an AMQP channel
%%% @end
%% @private
-module(turtle_service_mgr).
-behaviour(gen_server).
-include_lib("amqp_client/include/amqp_client.hrl").

%% Lifetime
-export([
    start_link/1
]).

%% API
-export([
    update_configuration/2
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
    conf,
    conn_ref
 }).

-define(DEFAULT_CONFIGURATION,
    #{ passive => false }).

%% LIFETIME MAINTENANCE
%% ----------------------------------------------------------
start_link(Configuration) ->
    MergedConf = maps:merge(?DEFAULT_CONFIGURATION, Configuration),
    gen_server:start_link(?MODULE, [MergedConf], []).

%% API
%% ----------------------------------------------------------
update_configuration(ServiceName, Config)->
    Pid = where(ServiceName),
    gen_server:call(Pid, {config_update, ServiceName, Config}, 20*1000).


where(ChannelName) ->
    gproc:where({n,l,{turtle, service_channel, ChannelName}}).

%% CALLBACKS
%% -------------------------------------------------------------------

%% @private
init([#{ connection := ConnName, name := Name } = Conf]) ->
    ok = validate_config(Conf),
    Ref = gproc:nb_wait({n,l,{turtle, connection, ConnName}}),
    ok = exometer:ensure([ConnName, Name, msgs], spiral, []),
    ok = exometer:ensure([ConnName, Name, latency], histogram, []),
    {ok, {initializing, Ref, Conf}}.

%% @private
handle_call({config_update, PoolName, Config}, _From, #state{} = State) ->
    ok = validate_config(Config),
    #{name := _Name,
      connection := _ConnName,
      function := _Fun,
      consume_queue := _Queue,
      subscriber_count := K } = Config,
    Pool = gproc:where({n,l,{turtle,service_pool, PoolName}}),
    Workers = turtle_subscriber_pool:get_children(Pool),
    lists:foreach(fun({_, WorkerPid, _, _}) ->
                          ok = turtle_subscriber_pool:stop_child(Pool, WorkerPid)
                  end, Workers),

    % Apply new configuration changes.
    add_subscribers(Pool, Config, K),
    {reply, ok, State#state{conf = Config}};
handle_call(Call, From, State) ->
    logger:warning("Unknown call from ~p: ~p", [From, Call]),
    {reply, {error, unknown_call}, State}.

%% @private
handle_cast(Cast, State) ->
    logger:warning("Unknown cast: ~p", [Cast]),
    {noreply, State}.

%% @private
handle_info({gproc, Ref, registered, {_, Pid, _}}, {initializing, Ref, #{ name := Name, subscriber_count := K } = Conf }) ->
    {Pool, _} = gproc:await({n,l,{turtle,service_pool, Name}}, 5000),
    add_subscribers(Pool, Conf, K),
    MRef = monitor(process, Pid),
    reg(Name),
    {noreply,
      #state {
        conn_ref = MRef,
        conf = Conf,
        name = Name }};
handle_info({'DOWN', MRef, process, _, Reason}, #state { conn_ref = MRef } = State) ->
    {stop, {error, {connection_down, Reason}}, State};
handle_info(Info, State) ->
    logger:warning("Unknown info msg: ~p", [Info]),
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
