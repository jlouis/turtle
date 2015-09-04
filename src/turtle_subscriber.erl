%%% @doc subscribe to a channel, consume messages, process messages by a function
%% This module will subscribe to a target channel and start consuming on the channel.
%% Once consumption is started, it will process each incoming message by invoking a
%% function on each incoming message.
%%% @end
%% @private
-module(turtle_subscriber).
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
	conn_name,
	name,
	invoke,
	invoke_state = init,
	handle_info = undefined,
	channel,
	consumer_tag
 }).

%% LIFETIME MAINTENANCE
%% ----------------------------------------------------------
start_link(Config) ->
    gen_server:start_link(?MODULE, [Config], []).
	
%% CALLBACKS
%% -------------------------------------------------------------------

%% @private
init([#{
        channel := Channel,
        consume_queue := Queue,
        function := Fun,
        connection := ConnName,
        name := Name } = Conf]) ->
    {ok, Tag} = turtle:consume(Channel, Queue),
    ok = exometer:ensure([ConnName, Name, msgs], spiral, []),
    ok = exometer:ensure([ConnName, Name, latency], histogram, []),
    {ok, #state {
        consumer_tag = Tag, 
        invoke = Fun,
        invoke_state = invoke_state(Conf),
        handle_info = handle_info(Conf),
        channel = Channel,
        conn_name = ConnName,
        name = Name }}.

%% @private
handle_call(Call, From, State) ->
    lager:warning("Unknown call from ~p: ~p", [From, Call]),
    {reply, {error, unknown_call}, State}.

%% @private
handle_cast(Cast, State) ->
    lager:warning("Unknown cast: ~p", [Cast]),
    {noreply, State}.

%% @private
handle_info(#'basic.consume_ok'{}, State) ->
    {noreply, State};
handle_info(#'basic.cancel_ok'{}, State) ->
    lager:info("Consumption canceled"),
    {stop, normal, State};
handle_info({#'basic.deliver' {delivery_tag = Tag, routing_key = Key}, Content},
	#state {
	  invoke = Fun, invoke_state = IState,
	  channel = Channel, conn_name = CN, name = N } = State) ->
    S = turtle_time:monotonic_time(),
    case handle_message(Fun, Key, Content, IState, Channel) of
        {ack, IState2} ->
           E = turtle_time:monotonic_time(),
           exometer:update([CN, N, msgs], 1),
           exometer:update([CN, N, latency],
             turtle_time:convert_time_unit(E-S, native, milli_seconds)),
           ok = amqp_channel:cast(Channel, #'basic.ack' { delivery_tag = Tag }),
           {noreply, State#state { invoke_state = IState2 }};
        reject ->
           exometer:update([CN, N, rejects], 1),
           ok = amqp_channel:cast(Channel, #'basic.reject' { delivery_tag = Tag, requeue=true }),
           {noreply, State};
        remove ->
           exometer:update([CN, N, removals], 1),
           ok = amqp_channel:cast(Channel, #'basic.reject' { delivery_tag = Tag, requeue = false}),
           {noreply, State};
        ok ->
           {noreply, State}
    end;
handle_info(Info, #state { handle_info = undefined } = State) ->
    lager:warning("Unknown info message: ~p", [Info]),
    {noreply, State};
handle_info(Info, #state { handle_info = HandleInfo, invoke_state = IState } = State) ->
    {ok, IState2} = HandleInfo(Info, IState),
    {noreply, State#state { invoke_state = IState2 }}.

%% @private
terminate(_Reason, _State) ->
    ok.

%% @private
code_change(_, State, _) ->
    {ok, State}.

%%
%% INTERNAL FUNCTIONS
%%
handle_message(Fun, Key,
	#amqp_msg {
	    payload = Payload,
	    props = #'P_basic' {
	        content_type = Type,
	        correlation_id = CorrID,
	        reply_to = ReplyTo }} = M, IState, Channel) ->
    try Fun(Key, Type, Payload, IState) of
        ack -> {ack, IState};
        {ack, IState2} -> {ack, IState2};
        {reply, CType, Msg} ->
            reply(Channel, CorrID, ReplyTo, CType, Msg),
            {ack, IState};
        {reply, CType, Msg, IState2} ->
            reply(Channel, CorrID, ReplyTo, CType, Msg),
            {ack, IState2};
        reject -> reject;
        remove -> remove;
        ok -> ok
    catch
        Class:Error ->
            lager:warning("Cannot handle message ~p: ~p:~p (BT: ~p)", [format_amqp_msg(M), Class, Error, erlang:get_stacktrace()]),
            remove
    end.
    
format_amqp_msg(#amqp_msg { payload = Payload, props = Props }) ->
    Pl = case byte_size(Payload) of
        K when K < 64 -> Payload;
        _ ->
            <<Cut:64/binary, _/binary>> = Payload,
            Cut
    end,
    {Pl, Props}.

%% Compute the initial state of the function
invoke_state(#{ init_state := S }) -> S;
invoke_state(_) -> init.

handle_info(#{ handle_info := Handler }) -> Handler;
handle_info(_) -> undefined.

reply(_Ch, _CorrID, undefined, _CType, _Msg) ->
    lager:warning("Replying to target with no reply-to queue defined"),
    ok;
reply(Ch, CorrID, ReplyTo, CType, Msg) ->
    Publish = #'basic.publish' {
        exchange = <<"amq.direct">>,
        routing_key = ReplyTo
    },
    Props = #'P_basic' { content_type = CType, correlation_id = CorrID },
    AMQPMsg = #amqp_msg { props = Props, payload = Msg},
    amqp_channel:cast(Ch, Publish, AMQPMsg).
