[![Build Status](https://travis-ci.org/shopgun/turtle.svg?branch=master)](https://travis-ci.org/shopgun/turtle)

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
  always a process knowing the connection state toward RabbitMQ. If
  the connection is down, you will receive errors back.

* Provide support for having multiple connection points to the
  RabbitMQ cluster. On failure the client will try the next client in
  the group. On too many failures on a group, connect to a backup
  group instead. This allows a client the ability to use a backup
  cluster in another data center, should the primary cluster break
  down.

* Support easy subscription where each received message is handled by
  a stateful callback function supplied by the application.

* Support easy message sending anywhere in the application by
  introducing a connection proxy.

* Support RPC style calls in RabbitMQ over the connection proxy. This
  allows a caller to block on an AMQP style RPC message since the
  connection proxy handles the asynchronous non-blocking behavior.

* Tracks timing of most calls. This allows you to gather metrics on
  the behavior of underlying systems and in turn handle erroneous
  cases proactively.

* Implements embeddable supervision trees which are added to your
  application. Handles errors gracefully and makes sure connections
  are reaped correctly toward the RabbitMQ cluster.

* Uses exometer to track statistics automatically for proxies and
  services. This allows one to gather information about the behavior
  of how the application uses RabbitMQ.


## Ideology

The Turtle application deliberately exposes the underlying RabbitMQ
configuration to the user. This allows the user to handle most
low-level RabbitMQ settings directly without having to learn a new
language of configuration.

Furthermore, the goal is to wrap the `rabbitmq-erlang-client`, provide
a helpful layer and then get out of the way. The goal is not to make a
replacement which covers every use case. Just common tasks in
RabbitMQ. That is, we aim to be a 80% solution, not a complete
solution to every problem you might have.


## What we do not solve

Very specific and intricate RabbitMQ connections are better handled
directly with the driver. This project aims to capture common use
cases and make them easier to handle so you have to handle less
low-level aspects of the driver.

Things have been added somewhat on-demand. There are definitely setups
which are outside the scope of this library/application. But it is
general enough to handle most messaging patterns we've seen over the
years when using RabbitMQ.

The `rabbitmq-erlang-driver` is a complex beast. Way too complex one
could argue. And it requires you to trap exits in order to handle it
correctly. We can't solve these problems inside the driver, but we can
raise a scaffold around the driver in order to protect the remainder
of the system against it.


## Performance considerations

Turtle has been tested in a simple localhost setup in which is can
50.000 RPC roundtrips per second. This is the current known lower
bound for the system, and given improvement in RabbitMQ and the Erlang
subsystem this number may become better over time. It is unlikely to
become worse.


# Changes

* Upcoming

  - Upgrade dependencies; exometer_core and lager

* *Version 1.9.5* - SSL and Dynamic Creation

    - Can now specify ssl_options in connection_config in config.system

    - Can now dynamically create Connections, Services, and Publishers
      during runtime instead of only in configuration or Supervisors:
    
        - turtle_conn:new(Name,Config)
        - turtle_service:new(Supervisor,ServiceSpec)
        - turtle_publisher:new(Supervisor,PublisherSpec)

* *Version 1.9.4* - Introduce Connection Name Validation

    - Connection names are now validated when a publisher or a service
      is created. So turtle now fails, if a service or publisher is
      started with a connection that is not defined in the
      `connection_config` section of turtle's application environment.

    - Upgrade to AMQP Client version 3.7.5, fixing builds on Erlang 19.
      This build bug was caused by the ranch_proxy_protocol hex package
      being packed using a buggy vendored version of erl_tar.

* *Version 1.9.3* - Dependency bump

    - Upgrade Turtle to the 3.7.4 version of AMQP Client.

* *Version 1.9.2* - Dependency bump

    - *DEPENDENT BUG* RabbitMQ in version 3.6.x uses the "mochinum"
      module in the system. They imported this module verbatim with no
      renaming. As a result, if you depend on any other package which
      eventually uses "mochinum", then that version of RabbitMQ cannot
      be used simultaneously and this breaks the system.

      The problem has been fixed in 3.7.x, where the module has been
      properly renamed. We've updated the dependencies accordingly.

* *Version 1.9.1* - Service fix

    - *BUG:* If the RabbitMQ AMQP client dies, the supervision trees
      created in applications from turtle were not correctly detecting
      the erroneous case. This leads to a no-service situation without
      crashing. The fix correctly monitors the AMQP clients behavior
      and crashes appropriate parts of the supervision trees if the
      error occurs.

* *Version 1.9.0* - Important major bugfix release

    - *SERIOUS BUG:* The RabbitMQ AMQP client has no notion of a
      controlling process, but Turtle assumed it did. The consequence
      is that errors in workers leads to a situation where you have
      channels in a "limbo" state: not known to Turtle, but known to
      the client and the AMQP server.

      Messages which have been prefetched into these channels can
      never be acked nor rejected, because the tag is lost in a
      mailbox of a crashing process. The system thus slowly fills up
      prefetch slots until the system comes to a halt.

      Note periodic connection resets would fix the bug. Periodic
      connection resets happens somewhat weekly on the Amazon AWS
      platform which is why we didn't find this bug earlier.

      The solution taken for Turtle is to introduce a `turtle_janitor`
      process which maintains the channels and connections that has
      been handed out. The janitor monitors and closes connections and
      channels upon failure.

    - *POTENTIAL INCOMPATIBIILTY:* As a result of introducing a
      janitorial process, we can remove most of the `trap_exit` from
      the turtle processes. They don't have to clean up themselves
      anymore since the janitor is doing it for them. All in all, the
      system becomes far more Erlang-idiomatic.

      The fallout, however, is that if you have relied on trap-exit in
      the process, you can't anymore, so one has to look out for this
      incompatible change.

    - Connections now supports global deadlines. A configuration of a
      connection can take a deadline timeout. If no connection is made
      within the deadline, the supervisor tree of turtle crashes. Once
      it has crashed 3 times, the Erlang node will terminate. That is,
      if you set a timeout of `300*1000` (5 minutes) then at 15
      minutes of no operation, the Erlang node will fail.

    - Use hex.pm packages
    - Build/Test via Travis CI

* *Version 1.8.1* - Maintenance release

    - The child spec validator had a bug where `callback/5` functions
      were rejected as wrong. These are correct in bulk mode, so
      accept both `/4` and `/5` for now.

* *Version 1.8.0* - Make await an officially supported feature

    - Support turtle:await/2 which allows your system to wait for the
      presence of a publisher. This is highly useful in startup
      scenarios, where parts of your system needs to wait on other
      parts of the system to be ready before they can operate.

* *Version 1.7.2* — Maintenance release

    - Use the `rand` module in Turtle from now on. Readies ourselves
      for Erlang/OTP 20

    - Use 'erlang-lager/lager' in version 3.2.4 in order to support
      the newly formed lager organization.

* *Version 1.7.1* — Erlang/OTP 19.x support

    - *Potential incompatibility:* The versions of `rabbit_common` and
      `amqp_client` are now locked to version 3.6.2, with a fix for
      Erlang/OTP 19 compiles. This means `turtle` can be used with the
      newest Erlang version. There has not been much testing as of
      this patch, but the purpose is to enable forward adoption.

* *Version 1.7.0* — Bulk messaging support

    - *Potential incompatibility:* The `handle_info/2` callback when
      doing subscription services can now be called in more
      situations. Make sure your callback handle unknown messages by
      ignoring them. Otherwise your service may fail.

    - **(Experimental)** Add support for handling messages in
      bulk. See the appropriate section in the README file in order to
      use it. For now, this feature is experimental and subject to
      change if something doesn't work out.

    - Implement a change where `turtle_cannel` is renamed into
      `turtle_service_mgr` and draws no channels by itself. Rather,
      channels is now the domain of `turtle_subscriber`. This change
      moves the channel scope onto individual processes, which opens
      the design space for further improvement.

* *Version 1.6.2* — Maintenance release

    - (Tim Stewart) Support more error cases in the connection
      loop. Tim has seen problems with the connection process on
      Docker instances, and retrying seem to make the trouble go
      away. The change also plugs the possibility of other connections
      failing.

    - (Sougat Chakraborty) Implementation towards dynamic publisher
      takeovers. This allows dynamic replacement of publishers with
      new publishers. In turn, one can manage a supervised child and
      replace it with newly updated configuration as time goes on.

* *Version 1.6.1* — Make exometer probes known earlier in the
  connection cycle.

    - exometer probes are now known at configuration time rather than
      when a connection is successful. This allows you to subscribe
      the probes into `exometer_report` earlier when you start up your
      application.

* *Version 1.6.0* — Feature release. There is a number of new features:

    - Support updating of service/consumer configuration while the
      system is running. This has been provided by MakeMeFree/m2f.

    - Support supplying Ports as strings. They will be converted to
      their integer-type as a result.

* *Version 1.5.3* — Fix a robustness problem with the supervisor
  tree. If the worker pools failed too often, the system would fail to
  escalate the error upwards through the supervisor trees. Key
  supervisors were erroneously declared as `transient` where they
  should have been declared as `permanent`.

* *Version 1.5.2* — Introduce the ability for workers to `{stop,
  Reason, …}`, which allows a worker to crash while simultaneously
  rejecting the messages it is processing. This avoids destroying the
  message which is currently in the queue, while allowing you to get
  back to an invariant state.

* *Version 1.5.1* — Fix a bug in which a crash in the subscribers
  handler function could lead to unacked message leaks under moderate
  to high load.

* *Version 1.5.0* — Support a `forced passive` mode on
  declarations. Setting this will force turtle to set passive creation
  on every exchange and queue. This is intended to be used in the case
  where you have pre-created queues and want to verify the existence
  of such queues.

* *Version 1.4.3* — Fix an error in the handling of crashing functions
  bound to a subscriber. If a subscriber function crashed, we would
  incorrectly supply a wrong return value which would crash the
  subscriber. In turn, the errorneous message would never be removed
  from the queue.

* *Version 1.4.2* — Introduce a new way to write child_specs through
  helper functions in their respective modules. Ensures we have a hook
  point later for better validation of parameters.

* *Version 1.4.1* — Documentation updates to make it easier to use the
  application and explain its purpose better.

* *Version 1.4.0* — First Open Source release. Every change will be
  relative to this release version.


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

Turtle uses `gproc` as its registry. Every connection, channel,
service of proxy is registered in gproc under `{n,l,{turtle,Kind,
Name}}` where `Kind` signifies what kind of registration it is for.

There are two kinds of children you can link into your applications
supervisor tree:

* Publishers - These form a connection endpoint/proxy toward
  RabbitMQ. They allow for publication by sending messages, and also
  for doing RPC calls over RabbitMQ. Usually, one publisher is enough
  for an Erlang node, even though every message factors through the
  publisher proxy. But in setups with lots of load, having multiple
  publishers may be necessary.

* Subscribers - These maintain a pool of workers which handle incoming
  messages on a Queue. Each worker runs a (stateful) callback module
  which allows the application to easily handle incoming requests
  through a processor function.


# Building

Turtle is built using rebar3. Turtle is built primarily for Erlang/OTP
release 18 and later, but it should be usable in release 17.5 as
well. You can build turtle with a simple

    make compile

or by using rebar directly:

    rebar3 compile


# Testing

The tests has a prerequisite which is a running `RabbitMQ` instance on
localhost port 5672`. In the Travis CI environment, we request such a
server to be running, so we can carry out tests against it, but for
local development, you will need to add such a server to the
environment.

Running tests is as simple as:

    make test


# Motivation

Turtle was created to alleviate a common need in many systems we
had. As we started to move more and more work into a messaging
platform based on RabbitMQ, common patterns emerged. This application
captures these common patterns and solves the low-level work once and
for all.

In addition, by factoring all users through a single implementation,
we have an obvious hook-point for statistics, and logging. And as we
fix errors, we fix those in all dependent applications.


# Usage

To use the turtle application, your `sys.config` must define a set of
connections to keep toward the RabbitMQ cluster. And you must provide
publishers/services into your own applications supervisor tree. We
will address each of these in a separate section.


## Configuration

Turtle is configured with a set of connector descriptors. These goes
into a `turtle` section inside your `sys.config`:

``` erlang
    %% Turtle configuration
    {turtle, [
        {connection_config, [
            #{
                conn_name => amqp_server,

                username => "phineas",
                password => "ferb",
                virtual_host => "/tri-state-area",

                deadline => 300000,
                connections => [
                    {main, [
                      {"amqp-1.danville.com", 5672 },
                      {"amqp-2.danville.com", 5672 } ]},
                    {backup, [
                      {"backup.doofenschmirtz.com", 5672 } ]} ]
            }]}
    ]},...
```

This will set up two connection groups. The first group is the
Danville group with two hosts and the second is the Doofenschmirtz
group. Turtle will try amqp-1 and amqp-2 in a round-robin fashion for
a while. Upon failure it will fall back to the backup group.

The `deadline` parameter tells the system when to globally fail the
connection attempts. If no connection is made within the deadline,
then the system will crash the connection process inside turtle. The
supervisor tree will restart it, but this happens at most 3 times
before the node is killed. E.g., setting a deadline of 1 minute (60000
milli-seconds) will make the turtle application fail at 3 minutes if
no connection can be made within that time-frame.

The `deadline` parameter can be omitted, in which case there is no
deadline.

*DISCUSSION:* We tend to keep our systems without a deadline and rely
on error logging to tell us that something is wrong. But it may be
wise to guarantee some kind of progress on the AMQP connection for a
system which relies on AMQP connections to be present before they can
properly operate. The thing you will be balancing is how long to wait
for a network to settle and AMQP to be available versus detecting a
problem with the connections of nodes. Setting a value too low is
likely pose problems when your system has dependencies among its
services. Setting it too high tells you there are problems too late.

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

``` erlang
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
```

Note that `AMQPArgs` takes a list of AMQP declarations which can be
used to create exchanges, queues, and so on. The publisher will verify
that each of these declarations succeed and will crash if that is not
the case.

Note the option `#{ confirms => true }` which is used to signify that
we want the exchange to confirm publications back to us. This is
automatically handled by the publisher if you use it for RPC style
messaging.

Also, note the option `#{ passive => false }`. You can force passive
declarations by setting this to `true` in which case declarations will
set the `passive` flag where applicable. This ensures you only verify
the presence of exchanges/queues on the broker rather than
idempotently create them. In a development setting, it is nice to
automatically create missing exchanges and queues, but in a production
system you may want to create them beforehand.

Once we have a publisher in our tree, we can use it through the
`turtle` module. The API of `turtle` is envisioned to be stable, so
using it is recommended over using the turtle publisher directly.

*Note:* The name `my_publisher` is registered under gproc with a name
unique to the turtle application.

To publish messages on `my_publisher` we write:

``` erlang
    turtle:publish(my_publisher,
        Exch,
        RKey,
        <<"text/json">>,
        Payload,
        #{ delivery_mode => persistent | ephemeral }).
```

This will asynchronously deliver a message on AMQP on the given
exchange, with a given routing key, a given Media Type and a given
Payload. The option list allows you to easily control if the delivery
should be persistent or ephemeral (the latter meaning that rabbitmq
may throw away the message on an error).


### Remote procedure calls

To enable the RPC mechanism, you must change the configuration of
publisher process like the following:

``` erlang
    %% AMQP Publisher
    Exch = …,
    …,
    AMQPDecls = [ … ],
    AMQPPoolChildSpec =
        turtle_publisher:child_spec(PublisherName, ConnName, AMQPDecls,
            #{
              confirms => true,
              passive => false,
              rpc => enable
             }),
```

In particular, the configuration adds the `rpc => enable` binding, but
is otherwise the same as the above publisher configuration. This makes
the publisher configure itself for retrival of RPC messages by
declaring an exclusive `reply_to` queue for itself.

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

``` erlang
    case turtle:rpc_sync(my_publisher, Exch, RKey, Ctype, Payload) of
      {error, Reason} -> ...;
      {ok, NTime, CType, Payload} -> ...
    end.
```

The response contains the `NTime` which is the time it took to process
the RPC for the underlying system.

To run an RPC call asynchronously, you first execute the `rpc/5` call:

``` erlang
    {ok, FToken, Time} =
      turtle:rpc(my_publisher, Exch, RKey, CType, Payload),
```

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

``` erlang
    {rpc_reply, FToken, NTime, ContentType, Payload}
```

Where `FToken` refers to the future in the confirmation call above,
`NTime` is the time it took for the message roundtrip, in `native`
representation, and `ContentType` and `Payload` is the result of the
RPC call.

To conveniently block and wait on such a message, you can call a
function:

``` erlang
    case turtle:rpc_await(my_publisher, FToken, Timeout) of
        {error, Reason} -> {error, Reason};
        {ok, NTime, ContentType, Payload} -> ...
    end,
```

This will also monitor `my_publisher`. If you want to monitor
yourself, you can use

``` erlang
    Pid = turtle_publisher:where(Publisher),
    MRef = monitor(process, Pid),
```

and then use the `turtle:rpc_await_monitor/3` call.

You can also call `turtle:rpc_cancel/2` to cancel futures and ignore
them. This doesn't carry any guarantee that the RPC won't be executed
however.


## Message Subscription

The "other" direction is when Turtle acts like a "server", receives
messages from RabbitMQ and processes them. In this setup, you provide
a callback function to the turtle system and it invokes this function
(in its own context) for each message that is received over RabbitMQ.

Turtle supports two messaging modes: single and bulk. First we
describe single-delivery, which is the default mode. In this mode,
each message is processed in isolation. The other mode, bulk, is
described later on and allows you to gather multiple messages into
your callback state and batch process them towards the other end.


### Single-mode subscription

Configuration allows you to set QoS parameters on the line and also
configure how many workers should be attached to the RabbitMQ queue
for the service. In turn, this provides a simple connection limiter if
one wishes to limit the amount of simultaneous work to carry out. In
many settings, some limit is in order to make sure you don't
accidentally flood the machine with more work than it can handle.

To configure a supervisor tree for receiving messages, you create a
child specification for it as in the following:

``` erlang
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
```

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

The `passive` flag works as in the case of the publisher: setting this
value to `true` forces exchange/queue creation to be passive such that
creation fails if the queue is not present already on the broker.

Operation works by initializing the system into the value given by
`init_state`. Then, every message is handled by the callback given in
`function`. Unrecognized messages to the process is handled by the
`handle_info` call. This allows you to handle events from the outside.

*Note:* A current design limitation is that you can only process one
message at a time. A truly asynchronous message processor could be
written, but we have not had the need yet.

The signature for the callback function is:

``` erlang
    loop(RoutingKey, ContentType, Payload, State) ->
        {remove, State} | {reject, State} | {stop, Reason, State}
        | {ack, State'} | {reply, CType, Payload, State}
```

The idea is that the `loop/4` function processes the message and
returns what should happen with the message:

* Returning `remove` will reject the message with no requeue. It is
  intended when you know no other node/process will be able to handle
  the message. For instance that the data is corrupted.

* Returning `reject` is a temporary rejection of the message. It goes
  back into the queue as per the RabbitMQ semantics and will be
  redelivered later with the redelivery flag set (we currently have no
  way to inspect the redelivery state in the framework)

* The `ack` return value is used to acknowledge successful processing
  of the message. It is used in non-RPC settings.

* The `reply` return is used to form an RPC reply back to the caller.
  This correctly uses the reply-to queue and correlation ID of the
  AMQP message to make a reply back to the caller. Its intended use is
  in RPC messaging.

* The `stop` return is used to crash the subscriber-worker somewhat
  gracefully.  It rejects the message as in `reject` but also stops
  the worker, so you can get back to a safe invariant state. The
  supplied `Reason` becomes the exit reason for the worker.

If a message is received by the subscription process it doens't
understand itself, it is forwarded to the `handle_info` function:

``` erlang
    handle_info(Info, State) -> {ok, State}.
```

It is intended to be used to handle a state change upon external
events.


### Bulk-mode subscription *(Experimental)*

*Note:* We think this feature is sound, but currently it has not seen
much use and thus, the API might change in the future. We will have to
address problems, surely, and some of those might alter the API.

Enabling bulk mode proceeds as single mode configuration, but sets the
`bulk` mode flag on the connection:

``` erlang
    Config = #{
        name => Name,
        function => fun CallbackMod:loop/5,
        handle_info => fun CallbackMod:handle_info/2,
        …,

        prefetch_count => PC,
        mode => bulk
    }
```

This setting will feed messages to your callback function, up to the
prefetch_count, `PC`.  Hence, it is advised to set a relatively large
prefetch count if batching is desirable. In `bulk` mode, the callback
function looks like:

``` erlang
    loop(RoutingKey, ContentType, Payload, Tag, State) -> {Cmds, State}
```

where `Cmds` is a list of (effectful) commands to apply to the
RabbitMQ system. The list can be empty, in which case no commands are
processed. Note that the loop function also contains an opaque `Tag`
for the incoming message. If you store that tag, you can use it in
commands later on in the sequence. The possible commands are:

* `{ack, Tag}` — ack the message with `Tag`.

* `{bulk_ack, Tag}` — ack *every* message up to and including
  `Tag`. Only sends a single message to RabbitMQ and is thus quite
  efficient.

* `{bulk_nack, Tag}` — A RabbitMQ extension which is the dual of
  bulk_ack: reject every message up to and including `Tag`.

* `{reject, Tag}` — Reject the message with `Tag` requeueing it.

* `{remove, Tag}` — Remove the mesage with `Tag` and do not requeue it
  (making dead-letter-exchange delivery happen)

* `{reply, Tag, ContentType, Payload}` — Reply to the message
  identified by `Tag` with a `Payload` and set its content type to
  `ContentType`.

In bulk-mode, the `handle_info/2` function is altered in two ways:
one, it can now respond with commands to execute, and it will retrieve
informational messages if the rabbitmq server goes away as well. The
invocation is like:

``` erlang
    handle_info(Info, State) -> {Cmds, State}.
```

Where `Cmds` is as above, and `Info` is either a normal Info-message
sent to the subscriber, or the tuple `{amqp_shutdown, Reason}`. In any
other `Reason` than `normal` it will not be possible to process any
commands, and the subscriber will warn you if you try.

*Example:* The bulk-mode allows you to micro-batch messages. Set the
prefetch count to 1000. When your loop function gets it's first
incoming message, you store it and its `Tag` in the `State`, and you
return `{[], NewState}` so the message is retained, but no ack is
sent.

When the first message is retrieved, you call `erlang:send_after(25,
self(), batch_timeout)` which sets up a 25ms timeout. Now, typically
two scenarios can happen:

* You retrieve 100 messages before the timeout.

* The timeout triggers.

In any of these cases, you send off the current batch and pick the
last `Tag` of all of these, cancel the timer if necessary[0], and
return `{[{bulk_ack, Tag}], NewState}`. In turn, you are now batching
messages.

More advanced batching will look at the response and figure out if any
messages in the batch failed. Then you can individually build up a
command list:

``` erlang
    Cmds = [{ack, Tag0}, {ack, Tag1}, {reject, Tag2}, {ack, Tag3}, {remove, Tag4}, …],
```

so you get the correct messages acked or rejected. You can also handle
bulk-reply the same way by replying to invididual messages.

[0] Use the `TimerRef` to get rid of timers that race you in the
mailbox, by ignoring timers which you did not expect.


# Operation and failure modes

This section describes the various failure modes of Turtle and how
they are handled.

The `turtle` application will try to keep connections to RabbitMQ at
all times. Failing connections restart the connector, but it also acts
like a simple circuit breaker if there is no connection. Hence
`turtle` provides the invariant:

> For each connection, there is a process. This process *may* have a
> connection to RabbitMQ, or it may not, if it is just coming up or
> has been disconnected.

Connections are registered in `gproc` so a user can grab the
connection state from there. Especially by registering for
non-blocking waits until a name (re-)appears.

If a publisher proxy fails, it will affect the supervisor tree in
which it is installed in the usual way. Currently active RPCs will be
gone. Hence it is advisable to install a monitor in the proxy, as is
the case in the exposition above. This will allow a caller blocked on
an RPC to react such that you can continue operation.

If a callback function in a service crashes, the corresponding process
in the subscription pool will stop abnormally. The errorneous message
will be *removed* permanently from the queue in order to ensure it
will not crash other workers bound to the queue. The assumption is
that a crashing function is a programming error which should be
fixed. If you need access to the message, configure RabbitMQ to use
dead-lettering on the queue which will send the message to the
dead-letter-exchange (DLX). You can then handle the failing message
separately on another queue. If the bound function crashes more than
100 times in an hour, it will reach max restart intensity and restart
that part of the supervisor tree.

If a connection dies, every publisher or service relying on it crashes
and reaps the supervisor tree accordingly.
