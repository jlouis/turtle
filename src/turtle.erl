%% @doc External API for the Turtle subsystem.
%%
%% The interface to the Turtle system for other subsystems. You probably want to
%% start a `turtle_publisher' and a `turtle_subscriber' in your application, and then
%% use the `publish/5' command in this module to publish messages. The subscriber
%% is used by providing a function to it, which is called whenever messages arrive on
%% the subscribed channel.
%%
%% Other functions in this module are low-level functions. They are not meant for use
%% in applications directly, but in the future, we may have to tip-toe ourselves through
%% a special case or two. Hence, they are still exported and documented so people know
%% they are there.
%%
%% @end
-module(turtle).
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

%% High level API
-export([
	publish/5, publish/6,
	publish_sync/5, publish_sync/6,
	rpc/5,
	rpc_await/3, rpc_await_monitor/3,
	rpc_cancel/2,
	rpc_sync/5, rpc_sync/6
]).

%% Lifetime API
-export([
    await/2, await/3
]).

%% Low level API
-export([
	declare/2, declare/3,
	open_channel/1, open_connection/1,
	consume/2, consume/3, cancel/2,
	qos/2
]).

%% Info API
-export([
	i/0,
	i/1
]).

-type channel() :: pid().
-type time() :: integer().
-type rpc_tag() :: { pid(), integer() }.

-export_type([time/0]).

%% -- API --------------------------------------------------


%% @equiv publish(Name, Exch, Key, ContentType, Payload, #{ delivery_mode => ephemeral })
-spec publish(Pub, Exchange, Key, CType, Payload) -> ok
	when
	  Pub :: atom(),
	  Exchange :: binary(),
	  Key :: binary(),
	  CType :: binary(),
	  Payload :: iodata().

publish(Pub, X, Key, CType, Payload) ->
    publish(Pub, X, Key, CType, Payload, #{ delivery_mode => ephemeral }).

%% @equiv publish_sync(Name, Exch, Key, ContentType, Payload, #{ delivery_mode => ephemeral })

-spec publish_sync(Pub, Exchange, Key, CType, Payload) -> {ack | nack, time()}
	when
	  Pub :: atom(),
	  Exchange :: binary(),
	  Key :: binary(),
	  CType :: binary(),
	  Payload :: iodata().

publish_sync(Pub, X, Key, CType, Payload) ->
    publish_sync(Pub, X, Key, CType, Payload, #{ delivery_mode => ephemeral }).

%% @doc publish(Name, Exch, Key, ContentType, Payload, Opts) publishes messages.
%%
%% This publication variant requires you to have started a publisher already through
%% the supervisor tree. It will look up the appropriate publisher under the given `Name',
%% and will publish on `Exch' with routing key `Key', content-type `ContentType' and the
%% given `Payload'.
%%
%% Options is a map of options. Currently we support:
%%
%% <ul>
%% <li>delivery_mode :: persistent | ephemeral (ephemeral is the default)</li>
%% </ul>
%%
%% Publication is asynchronous, so it never fails, but of course, if network conditions are
%% outright miserable, it may fail to publish the message.
%% @end
-spec publish(Pub, Exchange, Key, CType, Payload, Opts) -> ok
	when
	  Pub :: atom(),
	  Exchange :: binary(),
	  Key :: binary(),
	  CType :: binary(),
	  Payload :: iodata(),
	  Opts :: #{ atom() => any() }.
publish(Pub, X, Key, CType, Payload, Opts) ->
    turtle_publisher:publish(Pub, X, Key, CType, Payload, Opts).

%% @doc rpc/5 performs RPC calls over a publisher
%%
%% The call returns `{ok, Opaque, T}' where `Opaque' is an opaque token for the query,
%% and `T' is the time it took for confirmation in milli seconds.
%% @end

-spec rpc(Pub, Exch, Key, CType, Payload) -> {ok, Opaque, T}
	when
	  Pub :: atom(),
	  Exch :: binary(),
	  Key :: binary(),
	  CType :: binary(),
	  Payload :: iodata(),
	  Opaque :: rpc_tag(),
	  T :: time().
rpc(Pub, X, Key, CType, Payload) ->
    turtle_publisher:rpc_call(Pub, X, Key, CType, Payload, #{ delivery_mode => ephemeral }).

%% @doc rpc_await/3 awaits the response of an opaque value
%% @end

-spec rpc_await(Pub, Opaque, Timeout) -> {ok, T, CType, Payload} | {error, Reason}
	when
	    Pub :: atom(),
	    Opaque :: rpc_tag(),
	    Timeout :: non_neg_integer(),
	    T :: time(),
	    CType :: binary(),
	    Payload :: binary(),
	    Reason :: term().

rpc_await(Publisher, Opaque, Timeout) ->
    Pid = turtle_publisher:where(Publisher),
    MRef = monitor(process, Pid),
    case rpc_await_monitor(Opaque, Timeout, MRef) of
        {error, timeout} ->
            demonitor(MRef, [flush]),
            {error, timeout};
        {error, Reason} ->
            {error, Reason};
        Reply ->
            demonitor(MRef, [flush]),
            Reply
    end.

%% @doc rpc_await_monitor/3 awaits a response or a monitor timeout
%%
%% This variant allows you to reuse a monitor rather than setting a new one every
%% time on the publisher. One tends to be enough :)
%% @end
-spec rpc_await_monitor(Opaque, Timeout, MRef) ->
		{ok, T, CType, Payload} | {error, Reason}
	when
	    Opaque :: rpc_tag(),
	    Timeout :: non_neg_integer(),
	    MRef :: reference(),
	    T :: time(),
	    CType :: binary(),
	    Payload :: binary(),
	    Reason :: term().

rpc_await_monitor(Opaque, Timeout, MRef) ->
    receive
        {rpc_reply, Opaque, T, ContentType, Payload} ->
            {ok, T, ContentType, Payload};
        {'DOWN', MRef, process, _, Reason} ->
            {error, {publisher_down, Reason}}
    after Timeout ->
        {error, timeout}
    end.
    
%% @doc rpc_cancel/2 cancels an opaque message on the publisher
%% @end

-spec rpc_cancel(Publisher, Opaque) -> ok
	when
	    Publisher :: atom(),
	    Opaque :: rpc_tag().

rpc_cancel(Publisher, Opaque) ->
    ok = turtle_publisher:rpc_cancel(Publisher, Opaque),
    receive
        {rpc_reply, Opaque, _, _, _} -> ok
    after 0 ->
        ok
    end.

%% @equiv rpc_sync(Pub, X, Key, CType, Payload, #{ timeout => 5000 })

-spec rpc_sync(Pub, X, Key, CType, Payload) -> {ok, T, RCType, RPayload} | {error, Reason}
	when
	  Pub :: atom(),
	  X :: binary(),
	  Key :: binary(),
	  CType :: binary(),
	  Payload :: iodata(),
	  T :: time(),
	  RCType :: binary(),
	  RPayload :: binary(),
	  Reason :: term().

rpc_sync(Pub, X, Key, CType, Payload) ->
    rpc_sync(Pub, X, Key, CType, Payload, #{ timeout => 5000 }).

%% @doc rpc_sync/6 performs a synchronous RPC call over AMQP
%% @end
-spec rpc_sync(Pub, X, Key, CType, Payload, Opts) ->
		{ok, T, RCType, RPayload} | {error, Reason}
	when
	  Pub :: any(),
	  X :: binary(),
	  Key :: binary(),
	  CType :: binary(),
	  Payload :: iodata(),
	  Opts :: #{ atom() => any() },
	  T :: time(),
	  RCType :: binary(),
	  RPayload :: binary(),
	  Reason :: term().

rpc_sync(Pub, X, Key, CType, Payload, #{ timeout := Timeout }) ->
    {ok, Opaque, _T} = rpc(Pub, X, Key, CType, Payload),
    case rpc_await(Pub, Opaque, Timeout) of
        {ok, T2, RepCType, RepPayload} ->
             {ok, T2, RepCType, RepPayload};
        {error, timeout} ->
            ok = rpc_cancel(Pub, Opaque),
            {error, timeout};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc publish_sync/6 publishes messages synchronously
%%
%% This variant of publish, will publish the message synchronously to the broker, and
%% wait for the best effort of delivery guarantee. Without publisher confirms, this is the
%% point where the message is delivered to the TCP network stack. With confirms, the
%% call will block until the Broker either acks or nacks it.
%% @end
-spec publish_sync(Pub, Exchange, Key, CType, Payload, Opts) -> {ack | nack, time()}
	when
	  Pub :: atom(),
	  Exchange :: binary(),
	  Key :: binary(),
	  CType :: binary(),
	  Payload :: iodata(),
	  Opts :: #{ atom() => any() }.
publish_sync(Pub, X, Key, CType, Payload, Opts) ->
    turtle_publisher:publish_sync(Pub, X, Key, CType, Payload, Opts).

%% @doc i/0 returns information about turtle children in the system
%%
%% @end
-spec i() -> #{ atom() => any() }.

i() ->
    #{
        publishers => i(publishers)
    }.

-spec i(publishers) -> #{ any() => pid() }.

i(publishers) ->
    MS = ets:fun2ms(fun({{n,l,{turtle,publisher,Name}}, Pid, _}) -> {Name, Pid} end),
    Entries = gproc:select({local, names}, MS),
    maps:from_list(Entries).

await(publisher, Name) ->    
    await(publisher, Name, 5000).

await(publisher, Name, Timeout) ->
    turtle_publisher:await(Name, Timeout).

%% -- PRIVATE API ---------------------------------------------------




%% @doc consume/2 starts consumption on a channel with default parameters
%% @end
%% @private
consume(Channel, Queue) ->
    consume(Channel, Queue, false).

%% @doc consume/3 starts consumption on a channel with default parameters
%% @end
%% @private
consume(Channel, Queue, NoAck) ->
   Sub = #'basic.consume' { queue = Queue, no_ack = NoAck },
   #'basic.consume_ok' { consumer_tag = Tag } =
       amqp_channel:call(Channel, Sub),
   {ok, Tag}.


%% @doc cancel/2 stop consumption on a channel again.
%% @end
%% @private
cancel(Channel, Tag) ->
    Cancel = #'basic.cancel' { consumer_tag = Tag },
    #'basic.cancel_ok' {} = amqp_channel:call(Channel, Cancel),
    ok.


%% @doc qos/2 set QoS parameters on a queue according to configuration
%% @end
%% @private
qos(Ch, #{ prefetch_count := K }) ->
    #'basic.qos_ok'{} = amqp_channel:call(Ch, #'basic.qos' { prefetch_count = K }),
    ok;
qos(_Ch, _Conf) -> ok.

%% @doc open_channel/1 opens a channel on a given connection
%% This function will return a channel on a given connection, bound in
%% the system.
%% @end
%% @private
-spec open_channel(atom()) -> {ok, channel()} | {error, Reason}
    when Reason :: term().
open_channel(Name) ->
    turtle_janitor:open_channel(Name).

%% @private
open_connection(Network) ->
    turtle_janitor:open_connection(Network).

%% @doc declare(Ch, Decls) declares a list of idempotent setup for the Channel/RabbitMQ
%% The RabbitMQ declaration stack is nasty because it returns different results for different
%% kinds of declarations. To fix this, we have this helper routine which will interpret
%% the results of declarations.
%%
%% Returns `ok' on success, crashes if any of the declarations fail.
%% @end
%% @private
-type declaration() :: #'exchange.declare'{} | #'queue.declare'{}.

-spec declare(channel(), [declaration()]) -> ok.
declare(Channel, Decls) -> declare(Channel, Decls, #{ passive => false }).

declare(_Channel, [], _) -> ok;
declare(Channel, [#'exchange.declare' {} = Exch | Ds], #{ passive := false } = Opts) ->
    #'exchange.declare_ok'{} = amqp_channel:call(Channel, Exch),
    declare(Channel, Ds, Opts);
declare(Channel, [#'exchange.declare' {} = Exch | Ds], #{ passive := true} = Opts) ->
    #'exchange.declare_ok'{} = amqp_channel:call(Channel,
	Exch#'exchange.declare' { passive = true }),
    declare(Channel, Ds, Opts);
declare(Channel, [#'queue.declare' {} = Queue | Ds], #{ passive := false } = Opts) ->
    #'queue.declare_ok'{} = amqp_channel:call(Channel, Queue),
    declare(Channel, Ds, Opts);
declare(Channel, [#'queue.declare' {} = Queue | Ds], #{ passive := true } = Opts) ->
    #'queue.declare_ok'{} = amqp_channel:call(Channel,
        Queue#'queue.declare' { passive = true }),
    declare(Channel, Ds, Opts);
declare(Channel, [#'queue.bind' {} = Queue | Ds], Opts) ->
    #'queue.bind_ok'{} = amqp_channel:call(Channel, Queue),
    declare(Channel, Ds, Opts);
declare(Channel, [#'queue.unbind' {} = Queue | Ds], Opts) ->
    #'queue.unbind_ok'{} = amqp_channel:call(Channel, Queue),
    declare(Channel, Ds, Opts);
declare(Channel, [#'exchange.bind'{} = Exch | Ds], Opts) ->
    #'exchange.bind_ok'{} = amqp_channel:call(Channel, Exch),
    declare(Channel, Ds, Opts).

