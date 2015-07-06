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

%% Low level API
-export([
	declare/2,
	open_channel/1,
	publish/5, publish/6,
	consume/2
]).

-type channel() :: pid().

%% @doc open_channel/1 opens a channel on a given connection
%% This function will return a channel on a given connection, bound in
%% the system.
%% @end
-spec open_channel(atom()) -> {ok, channel()} | {error, Reason}
    when Reason :: term().
open_channel(Name) -> turtle_conn:open_channel(Name).

%% @doc declare(Ch, Decls) declares a list of idempotent setup for the Channel/RabbitMQ
%% The RabbitMQ declaration stack is nasty because it returns different results for different
%% kinds of declarations. To fix this, we have this helper routine which will interpret
%% the results of declarations.
%%
%% Returns `ok' on success, crashes if any of the declarations fail.
%% @end
-type declaration() :: #'exchange.declare'{} | #'queue.declare'{}.

-spec declare(channel(), [declaration()]) -> ok.
declare(_Channel, []) -> ok;
declare(Channel, [#'exchange.declare' {} = Exch | Ds]) ->
    #'exchange.declare_ok'{} = amqp_channel:call(Channel, Exch),
    declare(Channel, Ds);
declare(Channel, [#'queue.declare' {} = Queue | Ds]) ->
    #'queue.declare_ok'{} = amqp_channel:call(Channel, Queue),
    declare(Channel, Ds);
declare(Channel, [#'queue.bind' {} = Queue | Ds]) ->
    #'queue.bind_ok'{} = amqp_channel:call(Channel, Queue),
    declare(Channel, Ds).

%% @equiv publish(Name, Exch, Key, ContentType, Payload, #{})
publish(Pub, X, Key, CType, Payload) ->
    publish(Pub, X, Key, CType, Payload, #{ delivery_mode => ephermeral }).

%% @doc publish(Name, Exch, Key, ContentType, Payload, Opts) publishes messages.
%%
%% This publication variant requires you to have started a publisher already through
%% the supervisor tree. It will look up the appropriate publisher under the given `Name',
%% and will publish on `Exch' with routing key `Key', content-type `ContentType' and the
%% given `Payload'.
%%
%% Options is a map of options. Currently we support:
%%
%% * delivery_mode :: persistent | ephermeral (ephermeral is the default)
%%
%% Publication is asynchronous, so it never fails, but of course, if network conditions are
%% outright miserable, it may fail to publish the message.
%% @end
publish(Pub, X, Key, CType, Payload, Opts) ->
    turtle_publisher:publish(Pub, X, Key, CType, Payload, Opts).

%% @doc consume/2 starts consumption on a channel with default parameters
%% @end
consume(Channel, Queue) ->
   Sub = #'basic.consume' { queue = Queue },
   #'basic.consume_ok' { consumer_tag = Tag } =
       amqp_channel:call(Channel, Sub),
   {ok, Tag}.
