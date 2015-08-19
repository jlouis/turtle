# Turtle

The `turtle` application maintains RabbitMQ connections. Its purpose is to keep connections alive to a set of RabbitMQ clusters. Furthermore, other applications, using RabbitMQ, can request connectivity through the `turtle` application, so they don't have to maintain a lot of the stuff themselves.

## Testing

The tests has a prerequisite which is a running `rabbitmq-server`. In the Travis CI environment, we request such a server to be running, so we can carry out tests against it, but for local development, you will need to add such a server to the environment.

Running tests is as simple as:

	make test

## Motivation

We need to talk a lot of RabbitMQ if we are to start slowly hoisting work out of existing systems and into Erlang. In a transition phase, we need some way to communicate. RabbitMQs strength here is its topology-flexibility, not its speed. We don't think we'll hit any speed problem in RMQ for the forseeable future, but the flexibility of the platform is a nice thing to have.

We don't want to write low-level RabbitMQ stuff in each and every application. Hence, we provide an application/library that makes the invocation of RabbitMQ easier in our other projects. This library is called `turtle' 

## Configuration

Turtle is configured with a set of connector descriptions of the form:

	#{
		conn_name => …,
		
		%% RabbitMQ connection parameters
		username => …,
		password => …,
		host => …,
		%% Further RabbitMQ connection parameters
	}

This will configure a connection under the name `conn_name` with the given parameters. It is the intention that this configuration is static for the node onto which it is deployed. That is, the node should always maintain such a connection to a target.

In order to support environment configuration of this particular thing you can write, e.g.,

	#{
		username => {env, "JUNGO_AMQP_USERNAME", "guest"},
		…
	}
	
which will configure the username to be picked up from the environment under the name
of `JUNGO_AMQP_USERNAME` and if it is not present, it will pick `"guest"` as the username.

The reason this is not automatically configured is that the configuration can be pretty
complex and we probably want control in those situations over the name used in the
environment.

# Operation

The `turtle` application will try to keep connections to RabbitMQ at all times. Failing connections restart the connector, but it also acts like a simple circuit breaker if there is no connection. Hence `turtle` provides the invariant:

> For each connection, there is a process. This process *may* have a connection to RabbitMQ, or it may not, if it is just coming up or has been disconnected.

# Dynamic use of RabbitMQ in other applications

Using `turtle` is relatively simple. You start either a `publisher` or a `service` (or both in some cases). The publisher is a worker process and the service is a supervisor tree. They are meant to be linked into a target process. They will register themselves under names once they are up and running, which means you can await a valid connection in your tree. Also, they will terminate their trees if the connection goes away. Beware of this when you design your own supervisor tree.

## Publication

Publication is a simple "cast this message to the publisher" for now. Later on, it is a hook point for the ubiquitious Request/Reply pattern.

## Service subscription

This essentially binds a function to a queue with a given concurrency level. I.e., it spawn `K` workers and then starts consumption on the queue. For each message arriving, it will call the function you provide.

Again, the current setup is pretty rudimentary, but later on we can layer more complex stuff on top of this construction as we go along.


