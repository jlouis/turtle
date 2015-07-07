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
        name := Name }]) ->
    {ok, Tag} = turtle:consume(Channel, Queue),
    ok = exometer:ensure([ConnName, Name, msgs], spiral, []),
    ok = exometer:ensure([ConnName, Name, latency], histogram, []),
    {ok, #state {
        consumer_tag = Tag, 
        invoke = Fun,
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
	#state { invoke = Fun, channel = Channel, conn_name = CN, name = N } = State) ->
    S = turtle_time:monotonic_time(),
    case handle_message(Fun, Key, Content) of
        ack ->
           E = turtle_time:monotonic_time(),
           exometer:update([CN, N, msgs], 1),
           exometer:update([CN, N, latency],
              turtle_time:convert_time_unit(E - S, native, milli_seconds)),
           ok = amqp_channel:cast(Channel, #'basic.ack' { delivery_tag = Tag });
        ok ->
           ignore
    end,           
    {noreply, State};
handle_info(_, State) ->
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
handle_message(Fun, Key,
	#amqp_msg {
	    payload = Payload,
	    props = #'P_basic' { content_type = Type }} = M) ->
    try Fun(Key, Type, Payload) of
        ack -> ack
    catch
        Class:Error ->
            lager:warning("Cannot handle message ~p: ~p:~p", [format_amqp_msg(M), Class, Error]),
            ok
    end.
    
format_amqp_msg(#amqp_msg { payload = Payload, props = Props }) ->
    Pl = case byte_size(Payload) of
        K when K < 128 -> Payload;
        _ ->
            <<Cut:128, _/binary>> = Payload,
            Cut
    end,
    {Pl, Props}.
