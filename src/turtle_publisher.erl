%%% @doc The publisher is a helper for publishing messages on a channel
%%% @end
-module(turtle_publisher).
-behaviour(gen_server).
-include_lib("amqp_client/include/amqp_client.hrl").

%% Lifetime
-export([start_link/3,
         start_link/4,
         where/1,
         child_spec/4,
         await/2
]).

%% API
-export([
    publish/6,
    publish_sync/6,
    rpc_call/6,
    rpc_cancel/2,
    update_configuration/4
]).

%% gen_server Callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-record(track_db, {
    monitors = #{},
    live = #{}
}).

-record(state, {
    conn_name,
    name,
    channel,
    conn_ref,
    confirms,
    reply_queue,
    corr_id,
    consumer_tag,
    in_flight = #track_db{},
    unacked = gb_trees:empty()
 }).

-define(DEFAULT_OPTIONS,
    #{
        declarations => [],
        confirms => false,
        passive => false
    }).

%% LIFETIME MAINTENANCE
%% ----------------------------------------------------------

%% @doc Provide a child spec for OTP supervisor linkage
%%
%% The options are is as in {@link start_link/4}
%% @end
child_spec(Name, Conn, Decls, Options) ->
    {Name, {?MODULE, start_link, [Name, Conn, Decls, Options]},
        permanent, 5000, worker, [?MODULE]}.

%% @doc Start a new publication worker
%%
%% Provides an OTP gen_server for supervisor linkage. The `Name' is the name of
%% this publisher (which it registers itself in gproc as). The `Connection' is the turtle-name
%% for the connection, i.e., `amqp_server'. Finally, `Declarations' is a declaration list to
%% be executed against AMQP when setting up.
%% @end
start_link(Name, Connection, Declarations) ->
    ok = validate_connection_name(Connection),
    Options = maps:merge(?DEFAULT_OPTIONS, #{ declarations => Declarations}),
    gen_server:start_link(?MODULE, [Name, Connection, Options], []).

%% @doc start_link/4 starts a publisher with options
%%
%% This variant of start_link takes an additional `Options' section which can be
%% used to set certain publisher-specific options:
%% <dl>
%%   <dt>#{ confirms => true }</dt><dd>should be enable publisher confirms?</dd>
%%   <dt>#{ passive => true }</dt>
%%     <dd>Force queues and exchanges to be declared passively.</dd>
%% </dl>
%% @end
start_link(Name, Connection, Declarations, InOptions) ->
    ok = validate_connection_name(Connection),
    Options = maps:merge(?DEFAULT_OPTIONS, InOptions),
    gen_server:start_link(?MODULE,
        [Name, Connection, Options#{ declarations := Declarations }], []).

%% @doc
%% This variant of publisher dynamically updates configuration of
%% an existing publisher without downtime. It starts a publisher, looks up for
%% the owner of `Name' and notifies the old publisher. The old publisher gives away control to
%% new publisher and closes down
%% @end
start_link(takeover, Name, Connection, Declarations, InOptions) ->
    ok = validate_connection_name(Connection),
    Options = maps:merge(?DEFAULT_OPTIONS, InOptions),
    gen_server:start_link(?MODULE, [{takeover, Name}, Connection,
                                    Options#{declarations := Declarations }], []).


%% @doc where/1 returns what `Pid' a publisher is currently running under
%% @end
where(N) ->
    gproc:where({n,l,{turtle,publisher,N}}).

%% @doc await/2 waits for a publisher to come on-line or a timeout
%% @end
await(N, Timeout) ->
    gproc:await({n,l,{turtle,publisher,N}}, Timeout).

%% @doc publish a message asynchronously to RabbitMQ
%%
%% The specification is that you have to provide all parameters, because experience
%% has shown that you end up having to tweak these things quite a lot in practice.
%% Hence we provide the full kind of messaging, rather than a subset.
%%
%% @end
publish(Publisher, Exch, Key, ContentType, Payload, Opts) ->
    Pub = mk_publish(Exch, Key, ContentType, Payload, Opts),
    gen_server:cast(where(Publisher), Pub).

%% @doc publish a message synchronously to RabbitMQ
%%
%% A synchronous publish means the caller is blocked until the message is
%% on-the-wire toward the RabbitMQ server. It doesn't guarantee delivery, for
%% which you must use publisher confirms. The options are as in {@link publish/5}
%%
%% @end
publish_sync(Publisher, Exch, Key, ContentType, Payload, Opts) ->
    Pub = mk_publish(Exch, Key, ContentType, Payload, Opts),
    gen_server:call(where(Publisher), {call, Pub}).

%% @private
rpc_call(Publisher, Exch, Key, ContentType, Payload, Opts) ->
    Pub = mk_publish(Exch, Key, ContentType, Payload, Opts),
    gen_server:call(where(Publisher), {rpc_call, Pub}).

%% @private
rpc_cancel(Publisher, Opaque) ->
    gen_server:call(where(Publisher), {rpc_cancel, Opaque}).

update_configuration(Name, Connection, Decls, InOpts) ->
    start_link(takeover, Name, Connection, Decls, InOpts).

%% CALLBACKS
%% -------------------------------------------------------------------

%% @private
init([{takeover, Name}, ConnName, Options]) ->
    Ref = gproc:nb_wait({n,l,{turtle,connection, ConnName}}),
    ok = exometer:ensure([ConnName, Name, casts], spiral, []),
    {ok, {initializing_takeover, Name, Ref, ConnName, Options}};
init([Name, ConnName, Options]) ->
    %% Initialize the system in the {initializing,...} state and await the presence of
    %% a connection under the given name without blocking the process. We replace
    %% the state with a #state{} record once that happens (see handle_info/2)
    Ref = gproc:nb_wait({n,l,{turtle,connection,ConnName}}),
    ok = exometer:ensure([ConnName, Name, casts], spiral, []),
    {ok, {initializing, Name, Ref, ConnName, Options}}.

%% @private
handle_call(_Pub, _From, {initializing, _, _, _, _} = Init) ->
    {reply, {error, initializing}, Init};
handle_call(_Pub, _From, {initializing_takeover, _, _, _, _} = Init) ->
    {reply, {error, initializing_takeover}, Init};
handle_call({Kind, {publish, Pub, Props, Payload}}, From,
    #state {conn_name = ConnName, name = Name } = InState) ->
    Res =  publish({Kind, From}, Pub, #amqp_msg { props = Props, payload = Payload }, InState),
    exometer:update([ConnName, Name, Kind], 1),
    Res;
handle_call({rpc_cancel, {_Pid, CorrID}}, _From, #state { in_flight = IF } = State) ->
    {reply, ok, State#state { in_flight = track_cancel(CorrID, IF) }};
handle_call({transfer_ownership, Pid}, _From, #state{name = Name} = State) ->
    PubKey = {n,l,{turtle,publisher,Name}},
    gproc:give_away(PubKey, Pid),
    gproc:goodbye(),
    {stop, normal, ok, State};
handle_call(Call, From, State) ->
    lager:warning("Unknown call from ~p: ~p", [From, Call]),
    {reply, {error, unknown_call}, State}.

%% @private
handle_cast(Pub, {initializing, _, _, _, _} = Init) ->
    %% Messages cast to an initializing publisher are thrown away, but it shouldn't
    %% happen, so we log them
    lager:warning("Publish while initializing: ~p", [Pub]),
    {noreply, Init};
handle_cast(Pub, {initializing_takeover, _, _, _, _} = Init) ->
    lager:warning("Publish while takeover initialization: ~p", [Pub]),
    {noreply, Init};
handle_cast({publish, Pub, Props, Payload},
    #state { conn_name = ConnName, name = Name } = InState) ->
    case publish({cast, undefined}, Pub, #amqp_msg { props = Props, payload = Payload }, InState) of
        {noreply, State} ->
            exometer:update([ConnName, Name, casts], 1),
            {noreply, State};
        {reply, _Rep, State} ->
            exometer:update([ConnName, Name, casts], 1),
            {noreply, State}
    end;
handle_cast(Cast, State) ->
    lager:warning("Unknown cast: ~p", [Cast]),
    {noreply, State}.

%% @private
handle_info({gproc, Ref, registered, {_, Pid, _}}, {initializing, N, Ref, CName, Options}) ->
    {ok, Channel} = turtle:open_channel(CName),
    #{ declarations := Decls, passive := Passive, confirms := Confirms} = Options,
    ok = turtle:declare(Channel, Decls, #{ passive => Passive }),
    ok = turtle:qos(Channel, Options),
    ok = handle_confirms(Channel, Options),
    {ok, ReplyQueue, Tag} = handle_rpc(Channel, Options),
    ConnMRef = monitor(process, Pid),
    reg(N),
    {noreply,
      #state {
        channel = Channel,
        conn_ref = ConnMRef,
        conn_name = CName,
        confirms = Confirms,
        corr_id = 0,
        reply_queue = ReplyQueue,
        consumer_tag = Tag,
        name = N}};
handle_info({gproc, Ref, registered, {_, Pid, _}}, {initializing_takeover, N, Ref, CName, Options}) ->
    {ok, Channel} = turtle:open_channel(CName),
    #{ declarations := Decls, passive := Passive, confirms := Confirms} = Options,
    ok = turtle:declare(Channel, Decls, #{ passive => Passive }),
    ok = turtle:qos(Channel, Options),
    ok = handle_confirms(Channel, Options),
    {ok, ReplyQueue, Tag} = handle_rpc(Channel, Options),
    ConnMRef = monitor(process, Pid),
    case where(N) of
        undefined ->
            reg(N);
        ExistingOwner ->
            gen_server:call(ExistingOwner, {transfer_ownership, self()})
    end,
    {noreply,
      #state {
        channel = Channel,
        conn_ref = ConnMRef,
        conn_name = CName,
        confirms = Confirms,
        corr_id = 0,
        reply_queue = ReplyQueue,
        consumer_tag = Tag,
        name = N}};
handle_info(#'basic.ack' { delivery_tag = Seq, multiple = Multiple},
    #state { confirms = true } = InState) ->
    {ok, State} = confirm(ack, Seq, Multiple, InState),
    {noreply, State};
handle_info(#'basic.nack' { delivery_tag = Seq, multiple = Multiple },
    #state { confirms = true } = State) ->
    {ok, State} = confirm(nack, Seq, Multiple, State),
    {noreply, State};
handle_info({channel_closed, Ch, Reason}, #state { channel = Ch } = State) ->
    Exit = case Reason of
               normal -> normal;
               shutdown -> normal;
               {shutdown, _} -> normal;
               Err -> {amqp_channel_closed, Err}
           end,
    {stop, Exit, State};
handle_info({'DOWN', MRef, process, _, Reason}, #state { conn_ref = MRef } = State) ->
    {stop, {error, {connection_down, Reason}}, State};
handle_info({'DOWN', MRef, process, _, _Reason}, #state { in_flight = IF } = State) ->
    %% Remove in-flight monitor if the RPC caller goes away
    {noreply, State#state { in_flight = track_cancel_monitor(MRef, IF) }};
handle_info({#'basic.deliver' { delivery_tag = Tag}, Content}, State) ->
    handle_deliver(Tag, Content, State);
handle_info(#'basic.consume_ok'{}, State) ->
    {noreply, State};
handle_info(#'basic.cancel_ok'{}, State) ->
    lager:info("Consumption canceled"),
    {stop, normal, State};
handle_info(Info, State) ->
    lager:warning("Received unknown info msg: ~p", [Info]),
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
reg(Name) ->
    true = gproc:reg({n,l,{turtle,publisher, Name}}).

handle_confirms(Channel, #{ confirms := true }) ->
     #'confirm.select_ok' {} = amqp_channel:call(Channel, #'confirm.select'{}),
     ok = amqp_channel:register_confirm_handler(Channel, self());
handle_confirms(_, _) -> ok.

%% @doc handle_rpc/2 enables RPC on a publisher
%% Different ways of enabling RPC on the connection. If given `enable' it just creates
%% an anonymous queue. If given `{enable, Q}' It looks for that queue.
%% @end
handle_rpc(Channel, #{ rpc := {enable, Q}}) ->
    #'queue.declare_ok' {} = amqp_channel:call(Channel,
        #'queue.declare' { exclusive = true, auto_delete = true, durable = false, queue = Q }),
    {ok, Tag} = turtle:consume(Channel, Q),
    {ok, Q, Tag};
handle_rpc(Channel, #{ rpc := enable }) ->
    #'queue.declare_ok' { queue = Q } = amqp_channel:call(Channel,
        #'queue.declare' { exclusive = true, auto_delete = true, durable = false }),
    {ok, Tag} = turtle:consume(Channel, Q),
    {ok, Q, Tag};
handle_rpc(_, _) -> {ok, undefined, undefined}.

%% @doc handle_deliver/3 handles delivery of responses from RPC calls
%% @end
handle_deliver(Tag, #amqp_msg { payload = Payload, props = Props },
    #state { in_flight = IF, channel = Ch } = State) ->
    #'P_basic' { content_type = Type, correlation_id = <<CorrID:64/integer>> } = Props,
    ok = amqp_channel:cast(Ch, #'basic.ack' { delivery_tag = Tag }),
    case track_lookup(CorrID, IF) of
        {ok, Pid, T, IF2} ->
            T2 = turtle_time:monotonic_time(),
            Pid ! {rpc_reply, {self(), CorrID}, T2 - T, Type, Payload},
            {noreply, State#state { in_flight = IF2 }};
        not_found ->
            exo_update(State, missed_rpcs, 1),
            {noreply, State}
    end.

exo_update(#state { conn_name = ConnName, name = Name }, T, C) ->
    exometer:update([ConnName, Name, T], C).

%% Compute the properties of an AMQP message
properties(ContentType, #{ delivery_mode := persistent }) ->
    #'P_basic' { content_type = ContentType, delivery_mode = 2 };
properties(ContentType, #{ delivery_mode := ephemeral }) ->
    #'P_basic' { content_type = ContentType };
properties(ContentType, #{}) ->
    %% Default to the ephemeral delivery mode if nothing given
    #'P_basic' { content_type = ContentType }.

%% Create a new publish package
mk_publish(Exch, Key, ContentType, IODataPayload, Opts) ->
    Pub = #'basic.publish' {
        exchange = Exch,
        routing_key = Key
    },
    Props = properties(ContentType, Opts),

    %% Much to our dismay, the amqp_client will only accept payloads which are
    %% already flattened binaries. We claim to support general iodata() input, so
    %% we have to convert the payload into the right form, which the amqp_client
    %% understands

    Payload = iolist_to_binary(IODataPayload),
    {publish, Pub, Props, Payload}.

%% Perform the AMQP publication and track confirms
publish({cast, undefined}, Pub, AMQPMsg, #state{ channel = Ch, confirms = true } = State) ->
   _Seq = amqp_channel:next_publish_seqno(Ch),
   ok = amqp_channel:cast(Ch, Pub, AMQPMsg),
    {noreply, State};
publish({call, From}, Pub, AMQPMsg, #state{ channel = Ch,  confirms = true, unacked = UA } = State) ->
   Seq = amqp_channel:next_publish_seqno(Ch),
   ok = amqp_channel:call(Ch, Pub, AMQPMsg),
   T = turtle_time:monotonic_time(),
   {noreply, State#state{ unacked = gb_trees:insert(Seq, {From, T}, UA) }};
publish({rpc_call, From}, Pub, AMQPMsg,
    #state {
        channel = Ch,
        confirms = true,
        unacked = UA,
        in_flight = IF,
        corr_id = CorrID,
        reply_queue = ReplyQ } = State) ->
   Seq = amqp_channel:next_publish_seqno(Ch),
   #amqp_msg { props = Props } = AMQPMsg,
   WithReply = AMQPMsg#amqp_msg{ props = Props#'P_basic' {
       reply_to = ReplyQ,
       correlation_id = <<CorrID:64/integer>> }},
   ok = amqp_channel:call(Ch, Pub, WithReply),
   T = turtle_time:monotonic_time(),

   {noreply, State#state {
       corr_id = CorrID + 1,
       unacked = gb_trees:insert(Seq, {rpc, From, T, CorrID}, UA),
       in_flight = track(From, CorrID, T, IF) }};
publish({rpc_call, From}, Pub, AMQPMsg,
    #state { channel = Ch,  confirms = false, in_flight = IF, corr_id = CorrID, reply_queue = ReplyQ } = State) ->
   #amqp_msg { props = Props } = AMQPMsg,
   WithReply = AMQPMsg#amqp_msg{ props = Props#'P_basic' {
       reply_to = ReplyQ,
       correlation_id = <<CorrID:64/integer>> }},
   ok = amqp_channel:call(Ch, Pub, WithReply),
   T = turtle_time:monotonic_time(),

   Opaque = {self(), CorrID},
   {reply, {ok, Opaque}, State#state {
       corr_id = CorrID + 1,
       in_flight = track(From, CorrID, T, IF) }};
publish({F, _X}, Pub, AMQPMsg, #state{ channel = Ch, confirms = false } = State) ->
   ok = amqp_channel:F(Ch, Pub, AMQPMsg),
   {reply, ok, State}.

confirm(Reply, Seq, Multiple, #state { unacked = UA } = State) ->
    T2 = turtle_time:monotonic_time(),
    {Results, UA1} = remove_delivery_tags(Seq, Multiple, UA),
    reply_to_callers(T2, Reply, Results),
    {ok, State#state { unacked = UA1 }}.

remove_delivery_tags(Seq, false, Unacked) ->
    case gb_trees:lookup(Seq, Unacked) of
        {value, X} -> {[X], gb_trees:delete(Seq, Unacked)};
        none ->
            %% This can happen if for instance we have casted a message. That
            %% Particular SeqNo is not present among the unacked messages
            {[], Unacked}
    end;
remove_delivery_tags(Seq, true, Unacked) ->
    case gb_trees:is_empty(Unacked) of
        true -> {[], Unacked};
        false ->
            {Smallest, X, UA1} = gb_trees:take_smallest(Unacked),
            case Smallest > Seq of
                true ->
                    {[], Unacked};
                false ->
                    {Xs, FinalUA} = remove_delivery_tags(Seq, true, UA1),
                    {[X | Xs], FinalUA}
            end
    end.

reply_to_callers(_T2, _Reply, []) -> ok;
reply_to_callers(T2, Reply, [{From, T1} | Callers]) ->
    Window = turtle_time:convert_time_unit(T2 - T1, native, milli_seconds),
    gen_server:reply(From, {Reply, Window}),
    reply_to_callers(T2, Reply, Callers);
reply_to_callers(T2, ack, [{rpc, From, T1, CorrID} | Callers]) ->
    Window = turtle_time:convert_time_unit(T2 - T1, native, milli_seconds),
    Opaque = {self(), CorrID},
    gen_server:reply(From, {ok, Opaque, Window}),
    reply_to_callers(T2, ack, Callers);
reply_to_callers(T2, nack, [{rpc, From, _T, CorrID} | Callers]) ->
    Opaque = {self(), CorrID},
    gen_server:reply(From, {error, {nack, Opaque}}),
    reply_to_callers(T2, nack, Callers).

track({Pid, _CallMonitor}, CorrID, Now, #track_db { monitors = Ms, live = Ls } = DB) ->
    MRef = monitor(process, Pid),
    DB#track_db { monitors = maps:put(MRef, CorrID, Ms), live = maps:put(CorrID, {Pid, MRef, Now}, Ls) }.

track_lookup(CorrID, #track_db { monitors = Ms, live = Ls }) ->
    case maps:get(CorrID, Ls, not_found) of
        {Pid, MRef, T} ->
            demonitor(MRef, [flush]),
            {ok, Pid, T, #track_db { monitors = maps:remove(MRef, Ms), live = maps:remove(CorrID, Ls) }};
        not_found ->
            not_found
    end.

track_cancel(CorrID, #track_db { monitors = Ms, live = Ls } = DB) ->
    case maps:get(CorrID, Ls, not_found) of
        not_found -> DB;
        {_Pid, MRef, _T} ->
            demonitor(MRef, [flush]),
            DB#track_db { monitors = maps:remove(MRef, Ms), live = maps:remove(CorrID, Ls) }
    end.

track_cancel_monitor(MRef, #track_db { monitors = Ms, live = Ls } = DB) ->
    case maps:get(MRef, Ms, not_found) of
        not_found -> DB;
        CorrID ->
            DB#track_db { monitors = maps:remove(MRef, Ms), live = maps:remove(CorrID, Ls) }
    end.

validate_connection_name(Connection) ->
    ConfigList = application:get_env(turtle, connection_config, []),
    connection_ok(Connection, ConfigList).

connection_ok(_, []) ->
    undefined_conn;
connection_ok(Name, [#{ conn_name := Name } | _]) ->
    ok;
connection_ok(Name, [_ | ConfigList]) ->
    connection_ok(Name, ConfigList).
