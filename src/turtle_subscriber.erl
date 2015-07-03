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
	start_link/3
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
	invoke,
	channel,
	consumer_tag
 }).

%% LIFETIME MAINTENANCE
%% ----------------------------------------------------------
start_link(Channel, Fun, Queue) ->
    gen_server:start_link(?MODULE, [Channel, Fun, Queue], []).
	
%% CALLBACKS
%% -------------------------------------------------------------------

%% @private
init([Channel, Fun, Queue]) ->
    Sub = #'basic.consume' { queue = Queue },
    #'basic.consume_ok' { consumer_tag = Tag } =
        amqp_channel:call(Channel, Sub),
    {ok, #state { consumer_tag = Tag,  invoke = Fun, channel = Channel }}.

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
    {stop, normal, State};
handle_info({#'basic.deliver' {delivery_tag = Tag, routing_key = Key}, Content},
	#state { invoke = Fun, channel = Channel } = State) ->
    case handle_message(Fun, Key, Content) of
        ack ->
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
