# Turtle

The `turtle` application maintains RabbitMQ connections. Its purpose is to keep connections alive to a set of RabbitMQ clusters. Furthermore, other applications, using RabbitMQ, can request connectivity through the `turtle` application, so they don't have to maintain a lot of the stuff themselves.

## Configuration

Turtle is configured with a set of connector descriptions of the form:

	#{
		conn_name := …,
		
		username := …,
		password := …,
		host = …,
		%% Further RabbitMQ connection parameters
	}

This will configure a connection under the name `conn_name` with the given parameters. It is the intention that this configuration is static for the node onto which it is deployed. That is, the node should always maintain such a connection to a target.

The configuration is passed as part of the `gproc` environment, which means one can override where configuration is in the system by the use of gproc.

## Dynamic use of RabbitMQ in other applications

TODO…

