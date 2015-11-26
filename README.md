# Turtle - A wrapper on the RabbitMQ Erlang Client

The `turtle` application is built to be a wrapper around the RabbitMQ
standard Erlang driver. The purpose is to enable faster implementation
and use of RabbitMQ by factoring out common tasks into a specific
application targeted toward ease of use.

The secondary purpose is to make the Erlang client better behaving
toward an OTP setup. The official client makes lots of assumptions
which are not always true in an OTP setting.

The features of turtle are:

* Maintain RabbitMQ connections and automate re-connections if the
network is temporarily severed. Provides the invariant that there is
always a process knowing the connection state toward RabbitMQ. If the
connection is down, you will receive errors back.
* Provide support for having multiple connection points to the
RabbitMQ cluster. On failure the client will try the next client in
the group. On too many failures on a group, connect to a backup group
instead. This allows a client the ability to use a backup cluster in
another data center, should the primary cluster break down.
* Support easy subscription where each received message is handled by
a stateful callback function supplied by the application.
* Support easy message sending anywhere in the application by
introducing a connection proxy.
* Support RPC style calls in RabbitMQ over the connection proxy. This
allows a caller to block on an AMQP style RPC message since the
connection proxy handles the asynchronous non-blocking behavior.
* Tracks timing of most calls. This allows you to gather metrics on
the behavior of underlying systems and in turn handle erroneous cases
proactively.
* Implements embeddable supervision trees which are added to your application. Handles errors gracefully and makes sure connections are reaped correctly toward the RabbitMQ cluster.
* Uses exometer to track statistics automatically for proxies and services. This allows one to gather information about the behavior of how the application uses RabbitMQ.

## Ideology

The Turtle application deliberately exposes the underlying RabbitMQ configuration to the user. This allows the user to handle most low-level RabbitMQ settings directly without having to learn a new language of configuration.

Furthermore, the goal is to wrap the `rabbitmq-erlang-client`, provide a helpful layer and then get out of the way. The goal is not to make a replacement which covers every use case. Just common tasks in RabbitMQ. That is, we aim to be a 80% solution, not a complete solution to every problem you might have.

## What we do not solve

Very specific and intricate RabbitMQ connections are better handled
directly with the driver. This project aims to capture common use
cases and make them easier to handle so you have to handle less
low-level aspects of the driver.

Things have been added somewhat on-demand. There are definitely setups
which are outside the scope of this library/application. But it is
general enough to handle most messaging patterns we've seen
over the years when using RabbitMQ.

The `rabbitmq-erlang-driver` is a complex beast. Way too complex one
could argue. And it requires you to trap exits in order to handle it
correctly. We can't solve these problems inside the driver, but we can
raise a scaffold around the driver in order to protect the remainder
of the system against it.

## Performance considerations

Turtle has been tested in a simple localhost setup in which is can
50.000 RPC roundtrips per second. This is the current known lower bound for the
system, and given improvement in RabbitMQ and the Erlang subsystem
this number may become better over time. It is unlikely to become worse.

# Changes

* *Version 1.4.3* — Fix an error in the handling of crashing functions bound to a subscriber. If a subscriber function crashed, we would incorrectly supply a wrong return value which would crash the subscriber. In turn, the errorneous message would never be removed from the queue.
* *Version 1.4.2* — Introduce a new way to write child_specs through helper functions in their respective modules. Ensures we have a hook point later for better validation of parameters.
* *Version 1.4.1* — Documentation updates to make it easier to use the application and explain its purpose better.
* *Version 1.4.0* — First Open Source release. Every change will be relative to this release version.

# Architecture

Turtle is an OTP application with a single supervisor. Given a
configuration in `sys.config`, Turtle maintains connections to the
RabbitMQ clusters given in the configuration in this supervisor.

Every other part of turtle provides a supervisor tree/worker process
which can be linked into the supervisor tree of a client application.
This design makes sure that connections live and die with its
surrounding application. Monitors on the connection maintainers makes
sure the tree is correctly closed/stopped/restarted if a connection
dies.

Turtle uses `gproc` as its registry. Every connection, channel, service of proxy is registered in gproc under `{n,l,{turtle,Kind, Name}}` where `Kind` signifies what kind of registration it is for.

There are two kinds of children you can link into your applications
supervisor tree:

* Publishers - These form a connection endpoint/proxy toward RabbitMQ. They allow for publication by sending messages, and also for doing RPC calls over RabbitMQ. Usually, one publisher is enough for an Erlang node, even though every message factors through the publisher proxy. But in setups with lots of load, having multiple publishers may be necessary.
* Subscribers - These maintain a pool of workers which handle incoming messages on a Queue. Each worker runs a (stateful) callback module which allows the application to easily handle incoming requests through a processor function.

# Building

Turtle is built using rebar3. Turtle is built primarily for Erlang/OTP release 18 and later, but it should be usable in release 17.5 as well. You can build turtle with a simple

    make compile

or by using rebar directly:

    rebar3 compile

# Testing

The tests has a prerequisite which is a running `RabbitMQ` instance on localhost port 5672`. In the Travis CI environment, we request such a server to be running, so we can carry out tests against it, but for local development, you will need to add such a server to the environment.

Running tests is as simple as:

	make test

# Motivation

Turtle was created to alleviate a common need in many systems we had. As we started to move more and more work into a messaging platform based on RabbitMQ, common patterns emerged. This application captures these common patterns and solves the low-level work once and for all.

In addition, by factoring all users through a single implementation, we have an obvious hook-point for statistics, and logging. And as we fix errors, we fix those in all dependent applications.

# Usage

To use the turtle application, your `sys.config` must define a set of
connections to keep toward the RabbitMQ cluster. And you must provide
publishers/services into your own applications supervisor tree. We
will address each of these in a separate section.

## Configuration

Turtle is configured with a set of connector descriptors. These goes
into a `turtle` section inside your `sys.config`:

    %% Turtle configuration
    {turtle, [
        {connection_config, [
            #{
                conn_name => amqp_server,
                
                username => "phineas",
                password => "ferb",
                virtual_host => "/tri-state-area",
                
                connections => [
                    {main, [
                      {"amqp-1.danville.com", 5672 },
                      {"amqp-2.danville.com", 5672 } ]},
                    {backup, [
                      {"backup.doofenschmirtz.com", 5672 } ]} ]
            }]}
    ]},...

This will set up two connection groups. The first group is the
Danville group with two hosts and the second is the Doofenschmirtz
group. Turtle will try amqp-1 and amqp-2 in a round-robin fashion for
a while. Upon failure it will fall back to the backup group.

There is currently not auth-mechanism support, but this should be
fairly easy to add.

Once the configuration is given, you can configure applications to use
the connection name `amqp_server` and it will refer to connections
with that name. As such, you can add another connection to another
rabbitmq cluster and Turtle knows how to keep them apart.

## Message publication

Turtle provides a simple publication proxy which makes it easier to
publish messages on RabbitMQ for other workers. Here is how to use it.

First, you introduce the publisher into your own supervisor tree. For
instance by writing:

    %% AMQP Publisher
    Exch = <<"my_exchange">>,
    PublisherName = my_publisher,
    ConnName = amqp_server, %% As given above
    AMQPDecls = [
      #'exchange.declare' { exchange = Exch, type = <<"topic">>, durable = false }
    ],
    AMQPPoolChildSpec =
        turtle_publisher:child_spec(PublisherName, ConnName, AMQPDecls,
            #{ confirms => true, passive => false }),

Note that `AMQPArgs` takes a list of AMQP declarations which can be
used to create exchanges, queues, and so on. The publisher will verify
that each of these declarations succeed and will crash if that is not
the case.

Note the option `#{ confirms => true }` which is used to signify that we want the exchange to confirm publications back to us. This is automatically handled by the publisher if you
use it for RPC style messaging.

Also, note the option `#{ passive => false }`. You can force passive declarations by setting this to `true` in which case declarations will set the `passive` flag where applicable. This ensures you only verify the presence of exchanges/queues on the broker rather than idempotently create them. In a development setting, it is nice to automatically create missing exchanges and queues, but in a production system you may want to create them beforehand.

Once we have a publisher in our tree, we can use it through the
`turtle` module. The API of `turtle` is envisioned to be stable, so
using it is recommended over using the turtle publisher directly.

*Note:* The name `my_publisher` is registered under gproc with a
name unique to the turtle application.

To publish messages on `my_publisher` we write:

    turtle:publish(my_publisher,
        Exch,
        RKey,
        <<"text/json">>,
        Payload,
        #{ delivery_mode => persistent | ephemeral }).

This will asynchronously deliver a message on AMQP on the given
exchange, with a given routing key, a given Media Type and a given
Payload. The option list allows you to easily control if the delivery
should be persistent or ephemeral (the latter meaning that rabbitmq
may throw away the message on an error).

### Remote procedure calls

The RPC mechanism of the publisher might at first look a bit "off" but
there is a reason to the madness. Executing RPC calls runs in two
phases. First, you publish a Query. This results in a publication
confirmation being received back. This confirmation acts as a *future*
which you can wait on, or cancel later on. By splitting the work into
two phases, the API allows a process to avoid blocking on the RPC
calls should it wish to do so.

For your convenience, the RPC system provides a call for standard
blocking synchronous delivery which is based on the primitives we
describe below. To make an RPC call, execute:

    case turtle:rpc_sync(my_publisher, Exch, RKey, Ctype, Payload) of
      {error, Reason} -> ...;
      {ok, NTime, CType, Payload} -> ...
    end.

The response contains the `NTime` which is the time it took to process
the RPC for the underlying system.

To run an RPC call asynchronously, you first execute the `rpc/5` call:

    {ok, FToken, Time} =
      turtle:rpc(my_publisher, Exch, RKey, CType, Payload),
    
This returns an `FToken`, which is the future of the computation. It
also returns the value `Time` which is the number of milli-seconds it
took for the RabbitMQ server to confirm the publication.

The result is delivered into the mailbox of the application when it
arrives.

*Notes:*

* All RPC delivery is ephemeral. The reason for this is that we assume
  RPC messages are short-lived and if there is any kind of error, then
  we assume that systems can idempotently re-execute their RPC call.
  The semantics of RabbitMQ strongly suggests this is the safe bet, as
  guaranteed delivery is a corner case that may fail under network
  partitions.
* Since results are delivered into the mailbox, you may need to
  arrange for their arrival in your application if you consume every
  kind of message in the mailbox. In the following, we will describe
  the message format that gets delivered, so you are able to handle
  this.

A message is delivered once the reply happens. This message has the
following format:

    {rpc_reply, FToken, NTime, ContentType, Payload}

Where `FToken` refers to the future in the confirmation call above,
`NTime` is the time it took for the message roundtrip, in `native`
representation, and `ContentType` and `Payload` is the result of the
RPC call.

To conveniently block and wait on such a message, you can call a
function:

    case turtle:rpc_await(my_publisher, FToken, Timeout) of
        {error, Reason} -> {error, Reason};
        {ok, NTime, ContentType, Payload} -> ...
    end,

This will also monitor `my_publisher`. If you want to monitor
yourself, you can use

    Pid = turtle_publisher:where(Publisher),
    MRef = monitor(process, Pid),

and then use the `turtle:rpc_await_monitor/3` call.

You can also call `turtle:rpc_cancel/2` to cancel futures and ignore
them. This doesn't carry any guarantee that the RPC won't be executed
however.

## Message Subscription

The "other" direction is when Turtle acts like a "server", receives
messages from RabbitMQ and processes them. In this setup, you provide
a callback function to the turtle system and it invokes this function
(in its own context) for each message that is received over RabbitMQ.

Configuration allows you to set QoS parameters on the line and also
configure how many workers should be attached to the RabbitMQ queue
for the service. In turn, this provides a simple connection limiter if
one wishes to limit the amount of simultaneous work to carry out. In
many settings, some limit is in order to make sure you don't
accidentally flood the machine with more work than it can handle.

To configure a supervisor tree for receiving messages, you create a
child specification for it as in the following:

    Config = #{
      name => Name,
      connection => amqp_server,
      function => fun CallbackMod:loop/4,
      handle_info => fun CallbackMod:handle_info/2,
      init_state => #{ ... },
      declarations =>
          [#'exchange.declare' { exchange = Exch, type = <<"topic">>, durable = true },
           #'queue.declare' { queue = Q, durable = true },
           #'queue.bind' {
               queue = Q,
               exchange = Exch,
               routing_key = Bind }],
      subscriber_count => SC,
      prefetch_count => PC,
      consume_queue => Q,
      passive => false
    },

    ServiceSpec = turtle_service:child_spec(Config),

This configures the `Service` to run. It will first use the
`declarations` section to declare AMQP queues and bindings. Then it
will set QoS options, notably the `prefetch_count` which by far is the
most important options for tuning RabbitMQ. By prefetching a bit of
messages in, you can hide the network latency for a worker, which
speeds up handling considerably. Even fairly low prefetch settings
tend to have good results. You also set a `subscriber_count` which
tells the system how many worker processes to spawn. This can be used
as a simple way to introduce an artificial concurrency limit into the
system. Some times this is desirable as you can avoid the system
flooding other subsystems.

The `passive` flag works as in the case of the publisher: setting this value to `true` forces exchange/queue creation to be passive such that creation fails if the queue is not present already on the broker.

Operation works by initializing the system into the value given by
`init_state`. Then, every message is handled by the callback given in
`function`. Unrecognized messages to the process is handled by the
`handle_info` call. This allows you to handle events from the outside.

*Note:* A current design limitation is that you can only process one
message at a time. A truly asynchronous message processor could be
written, but we have not had the need yet.

The signature for the callback function is:

    loop(RoutingKey, ContentType, Payload, State) ->
        {remove, State} | {reject, State}
        | {ack, State'} | {reply, CType, Payload, State}

The idea is that the `loop/4` function processes the message and
returns what should happen with the message:

* Returning `remove` will reject the message with no requeue. It is
  intended when you know no other node/process will be able to handle
  the message. For instance that the data is corrupted.
* Returning `reject` is a temporary rejection of the message. It goes
  back into the queue as per the RabbitMQ semantics and will be
  redelivered later with the redelivery flag set (we currently have no
  way to inspect the redelivery state in the framework)
* The `ack` return value is used to acknowledge successful processing of
  the message. It is used in non-RPC settings.
* The `reply` return is used to form an RPC reply back to the caller.
  This correctly uses the reply-to queue and correlation ID of the
  AMQP message to make a reply back to the caller. Its intended use is
  in RPC messaging.

If a message is received by the subscription process it doens't
understand itself, it is forwarded to the `handle_info` function:

    handle_info(Info, State) -> {ok, State}.

It is intended to be used to handle a state change upon external
events.

# Operation and failure modes

This section describes the various failure modes of Turtle and how they are handled.

The `turtle` application will try to keep connections to RabbitMQ at
all times. Failing connections restart the connector, but it also acts
like a simple circuit breaker if there is no connection. Hence
`turtle` provides the invariant:

> For each connection, there is a process. This process *may* have a
> connection to RabbitMQ, or it may not, if it is just coming up or
> has been disconnected.

Connections are registered in `gproc` so a user can grab the
connection state from there. Especially by registering for non-blocking waits until a name (re-)appears.

If a publisher proxy fails, it will affect the supervisor tree in which it is installed in the usual way. Currently active RPCs will be gone. Hence it is advisable to install a monitor in the proxy, as is the case in the exposition above. This will allow a caller blocked on an RPC to react such that you can continue operation.

If a callback function in a service crashes, the corresponding process in the subscription pool will stop abnormally. The errorneous message will be *removed* permanently from the queue in order to ensure it will not crash other workers bound to the queue. The assumption is that a crashing function is a programming error which should be fixed. If you need access to the message, configure RabbitMQ to use dead-lettering on the queue which will send the message to the dead-letter-exchange (DLX). You can then handle the failing message separately on another queue. If the bound function crashes more than 100 times in an hour, it will reach max restart intensity and restart that part of the supervisor tree.

If a connection dies, every publisher or service relying on it crashes and reaps the supervisor tree accordingly.
