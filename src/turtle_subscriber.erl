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

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-record(state,
        {conn_name,
         name,
         invoke,
         invoke_state = init,
         handle_info = undefined,
         channel :: undefined | pid(),
         monitor :: reference(),
         consumer_tag,
         mode = single
        }).

%% LIFETIME MAINTENANCE
%% ----------------------------------------------------------
start_link(Config) ->
    gen_server:start_link(?MODULE, [Config], []).
	
%% CALLBACKS
%% -------------------------------------------------------------------

%% @private
init([#{
        consume_queue := Queue,
        function := Fun,
        connection := ConnName,
        name := Name,
        passive := Passive,
        declarations := Decls } = Conf]) ->
    {ok, Ch} = turtle:open_channel(ConnName),
    MRef = erlang:monitor(process, Ch),
    ok = turtle:qos(Ch, Conf),
    ok = amqp_channel:register_return_handler(Ch, self()),
    ok = turtle:declare(Ch, Decls, #{ passive => Passive }),
    {ok, Tag} = turtle:consume(Ch, Queue),
    Mode = mode(Conf),
    {ok, #state {
            consumer_tag = Tag, 
            invoke = Fun,
            invoke_state = invoke_state(Conf),
            handle_info = handle_info(Conf),
            channel = Ch,
            monitor = MRef,
            conn_name = ConnName,
            name = Name,
            mode = Mode
           }}.

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
    {stop, normal, shutdown(rabbitmq_gone, State)};
handle_info({#'basic.deliver'{}, _Content} = Msg, #state { mode = single } = State) ->
    handle_deliver_single(Msg, State);
handle_info({#'basic.deliver'{}, _Content} = Msg, #state { mode = bulk } = State) ->
    handle_deliver_bulk(Msg, State);
handle_info(#'basic.return' {} = Return, #state { name = Name } = State) ->
    lager:info("Channel ~p received a return from AMQP: ~p", [Name, Return]),
    {noreply, State};
handle_info({channel_closed, Ch, Reason}, #state { channel = Ch } = State) ->
    Exit = case Reason of
               normal -> normal;
               shutdown -> normal;
               {shutdown, _} -> normal;
               Err -> {amqp_channel_died, Err}
           end,
    {stop, Exit, shutdown(Exit, State#state { channel = undefined })};
handle_info({'DOWN', MRef, process, _Pid, _Reason}, #state { monitor = MRef } = State) ->
    {stop, channel_died, State};
handle_info(Info, #state { handle_info = undefined } = State) ->
    lager:warning("Unknown info message: ~p", [Info]),
    {noreply, State};
handle_info(Info, #state { handle_info = HandleInfo, invoke_state = IState } = State) ->
    S = erlang:monotonic_time(),
    try HandleInfo(Info, IState) of
        {ok, IState2} -> {noreply, State#state { invoke_state = IState2 }};
        {Cmds, IState2} when is_list(Cmds) ->
            handle_commands(S, Cmds, State#state { invoke_state = IState2 })
    catch
        Class:Error ->
            lager:error("Handle info crashed: {~p, ~p}, stack: ~p",
                [Class, Error, erlang:get_stacktrace()]),
            {stop, {Class, Error}, State}
    end.

%% @private
terminate(_, #state { consumer_tag = Tag, channel = Ch }) when is_pid(Ch) ->
    %% If a wise soul calls gen_server:stop on us, we can still drain the messages
    %% out of the channel queue explicitly. We'll try this as part of stopping
    ok = turtle:cancel(Ch, Tag),
    ok = await_cancel_ok(),
    %% Once we know we have cancellation, drain the queue of the remaining
    %% messages.
    drain_reject_messages(Ch),
    %% Unregister ourselves the return handler. If the channel is gone, this will fail, but
    %% this is not going to be a problem.
    ok = amqp_channel:unregister_return_handler(Ch),
    %% Janitor will close the channel
    ok;    
terminate(_Reason, _State) ->
    %% If we are in a different state, we can't do anything nice. Just die.
    ok.

%% @private
code_change(_, State, _) ->
    {ok, State}.

%%
%% INTERNAL FUNCTIONS
%%

handle_deliver_bulk({#'basic.deliver' {delivery_tag = DTag, routing_key = Key},
	#amqp_msg {
	    props = #'P_basic' {
	        correlation_id = CorrID,
	        reply_to = ReplyTo }} = Content},
	#state {
	  invoke = Fun, invoke_state = IState,
	  channel = Channel,
	  conn_name = CN,
	  name = N } = State) ->
    S = erlang:monotonic_time(),
    Tag = {DTag, ReplyTo, CorrID},
    try handle_message(Tag, Fun, Key, Content, IState) of
        {[], S2} ->
            E = erlang:monotonic_time(),
            exometer:update([CN, N, msgs], 1),
            exometer:update([CN, N, latency],
                erlang:convert_time_unit(E-S, native, milli_seconds)),
            {noreply, State#state { invoke_state = S2 }};
        {Cmds, S2} when is_list(Cmds) ->
            handle_commands(S, Cmds, State#state { invoke_state = S2 })
    catch
        Class:Error ->
           lager:error("Handler function crashed: {~p, ~p}, stack: ~p, content: ~p",
               [Class, Error, erlang:get_stacktrace(), format_amqp_msg(Content)]),
           lager:error("Mailbox size ~p", [erlang:process_info(self(), message_queue_len)]),
           ok = amqp_channel:call(Channel, #'basic.reject' { delivery_tag = Tag, requeue = false }),
           {stop, {Class, Error}, State}
    end.
        
handle_deliver_single({#'basic.deliver' {delivery_tag = DTag, routing_key = Key},
		#amqp_msg {
	    props = #'P_basic' {
	        correlation_id = CorrID,
	        reply_to = ReplyTo }} = Content},
	#state { invoke = Fun, invoke_state = IState,channel = Channel } = State) ->
    S = erlang:monotonic_time(),
    Tag = {DTag, ReplyTo, CorrID},
    try
        %% Transform a single message into the style of bulk messages
        %% by temporarily inserting an empty tag
        SingleTag = {undefined, ReplyTo, CorrID},
        {Cmds, S2} = case handle_message(SingleTag, Fun, Key, Content, IState) of
            {ack, IState2} -> {[{ack, Tag}], IState2};
            {reply, _Tag, CType, Msg, IState2} -> {[{reply, Tag, CType, Msg}], IState2};
            {reject, IState2} -> {[{reject, Tag}], IState2};
            {remove, IState2} -> {[{remove, Tag}], IState2};
            {stop, Reason, IState2} -> {[{{stop, Reason}, Tag}], IState2};
            {ok, IState2} -> {[], IState2}
        end,
        handle_commands(S, Cmds, State#state { invoke_state = S2 })
    catch
        Class:Error ->
           lager:error("Handler function crashed: {~p, ~p}, stack: ~p, content: ~p",
               [Class, Error, erlang:get_stacktrace(), format_amqp_msg(Content)]),
           lager:error("Mailbox size ~p", [erlang:process_info(self(), message_queue_len)]),
           ok = amqp_channel:call(Channel, #'basic.reject' { delivery_tag = DTag, requeue = false }),
           {stop, {Class, Error}, State}
    end.

handle_commands(_S, [], State) ->
    {noreply, State};
handle_commands(S, [C | Next],
	#state { channel = Channel, conn_name = CN, name = N } = State) ->
    case C of
        {ack, Tag} ->
            E = erlang:monotonic_time(),
            exometer:update([CN, N, msgs], 1),
            exometer:update([CN, N, latency],
                erlang:convert_time_unit(E-S, native, milli_seconds)),
           ok = amqp_channel:cast(Channel, #'basic.ack' { delivery_tag = delivery_tag(Tag) }),
           handle_commands(S, Next, State);
       {bulk_ack, Tag} ->
            E = erlang:monotonic_time(),
            exometer:update([CN, N, msgs], 1),
            exometer:update([CN, N, latency],
                erlang:convert_time_unit(E-S, native, milli_seconds)),
           ok = amqp_channel:cast(Channel, #'basic.ack' { delivery_tag = delivery_tag(Tag), multiple = true }),
           handle_commands(S, Next, State);
       {bulk_nack, Tag} ->
           E = erlang:monotonic_time(),
            exometer:update([CN, N, msgs], 1),
            exometer:update([CN, N, latency],
                erlang:convert_time_unit(E-S, native, milli_seconds)),
           ok = amqp_channel:cast(Channel, #'basic.nack' { delivery_tag = delivery_tag(Tag), multiple = true }),
           handle_commands(S, Next, State);
       {reject, Tag} ->
           exometer:update([CN, N, rejects], 1),
           ok = amqp_channel:cast(Channel,
           	#'basic.reject' { delivery_tag = delivery_tag(Tag), requeue=true }),
           handle_commands(S, Next, State);
        {remove, Tag} ->
           exometer:update([CN, N, removals], 1),
           ok = amqp_channel:cast(Channel,
           	#'basic.reject' { delivery_tag = delivery_tag(Tag), requeue = false}),
           handle_commands(S, Next, State);
        {reply, Tag, CType, Msg} ->
            E = erlang:monotonic_time(),
            exometer:update([CN, N, msgs], 1),
            exometer:update([CN, N, latency],
                erlang:convert_time_unit(E-S, native, milli_seconds)),
           reply(Channel, Tag, CType, Msg),
           ok = amqp_channel:cast(Channel, #'basic.ack' { delivery_tag = delivery_tag(Tag) }),
           handle_commands(S, Next, State);

        {{stop, Reason}, Tag} ->
            ok = amqp_channel:cast(Channel,
            	#'basic.reject' { delivery_tag = delivery_tag(Tag), requeue = true }),
            {stop, Reason, State}
    end.

handle_message(Tag, Fun, Key,
	#amqp_msg {
	    payload = Payload,
	    props = #'P_basic' {
	        content_type = Type }}, IState) ->
    Res = case Tag of
        {undefined, _, _} -> Fun(Key, Type, Payload, IState);
        Tag -> Fun(Key, Type, Payload, Tag, IState)
    end,
    case Res of
        %% Bulk messages
        L when is_list(L) -> {L, IState};
        {L, IState2} when is_list(L) -> {L, IState2};
        
        %% Single messages
        ack -> {ack, IState};
        {ack, IState2} -> {ack, IState2};
        {reply, CType, Msg} -> {reply, Tag, CType, Msg, IState};
        {reply, CType, Msg, IState2} -> {reply, Tag, CType, Msg, IState2};
        reject -> {reject, IState};
        {reject, IState2} -> {reject, IState2};
        remove -> {remove, IState};
        {remove, IState2} -> {remove, IState2};
        {stop, Reason, IState2} -> {stop, Reason, IState2};
        {ok, IState2} -> {ok, IState2};
        ok -> {ok, IState}
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

reply(_Ch, {_Tag, undefined, _CorrID}, _CType, _Msg) ->
    lager:warning("Replying to target with no reply-to queue defined"),
    ok;
reply(Ch, {_Tag, ReplyTo, CorrID}, CType, Msg) ->
    Publish = #'basic.publish' {
        exchange = <<>>,
        routing_key = ReplyTo
    },
    Props = #'P_basic' { content_type = CType, correlation_id = CorrID },
    AMQPMsg = #amqp_msg { props = Props, payload = Msg},
    amqp_channel:cast(Ch, Publish, AMQPMsg).

await_cancel_ok() ->
    receive
       #'basic.cancel_ok'{} ->
           ok
    after 500 ->
           lager:error("No basic.cancel_ok received"),
           not_cancelled
    end.

drain_reject_messages(Channel) ->
    receive
        {#'basic.deliver' {delivery_tag = Tag }, _Content} ->
            ok = amqp_channel:call(Channel,
                #'basic.reject' { delivery_tag = Tag, requeue = true }),
            drain_reject_messages(Channel)
    after 0 ->
        ok
    end.

mode(#{ mode := bulk }) -> bulk;
mode(#{ mode := single }) -> single;
mode(#{}) -> single.

%% Placeholder for later
delivery_tag({Tag, _ReplyTo, _CorrID}) -> Tag.

shutdown(Reason, #state { handle_info = HandleInfo, invoke_state = IState } = State) ->
    S = erlang:monotonic_time(),
    try HandleInfo({amqp_shutdown, Reason}, IState) of
        {ok, IState2} -> {noreply, State#state { invoke_state = IState2 }};
        {Cmds, IState2} when is_list(Cmds) ->
            {stop, Reason,
               shutdown_process_commands(
                   S,
                   Cmds,
                   State#state { invoke_state = IState2 },
                   Reason)}
    catch
        Class:Error ->
            lager:error("Handle info crashed: {~p, ~p}, stack: ~p",
                [Class, Error, erlang:get_stacktrace()]),
            {stop, {Class, Error}, State}
    end.

shutdown_process_commands(_S, [], State, _Reason) ->
    State;
shutdown_process_commands(_S, Cmds, State, Reason) ->
    lager:warning("Ignoring ~B commands due to ungraceful shutdown: ~p",
        [length(Cmds), Reason]),
    State.

