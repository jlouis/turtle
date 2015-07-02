-module(turtle).
-include_lib("amqp_client/include/amqp_client.hrl").

%% Low level API
-export([
	declare/2,
	open_channel/1,
	publish/5,
	consume/3
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
declare(_Channel, []) ->
    ok;
declare(Channel, [#'exchange.declare' {} = Exch | Ds]) ->
    #'exchange.declare_ok'{} = amqp_channel:call(Channel, Exch),
    declare(Channel, Ds);
declare(Channel, [#'queue.declare' {} = Queue | Ds]) ->
    #'queue.declare_ok'{} = amqp_channel:call(Channel, Queue),
    declare(Channel, Ds);
declare(Channel, [#'queue.bind' {} = Queue | Ds]) ->
    #'queue.bind_ok'{} = amqp_channel:call(Channel, Queue),
    declare(Channel, Ds).

%% @doc publish(Chan, Exch, Key, Payload) publishes messages on a channel
%% Low-level helper. Publish a `Payload' on the exchange `Exch' with routing key `Key'
%% @end
publish(Pub, X, Key, CType, Payload) ->
    turtle_publisher:publish(Pub, X, Key, CType, Payload).


consume(Channel, Queue, ConsumerPid) ->
   Sub = #'basic.consume' { queue = Queue },
   #'basic.consume_ok' { consumer_tag = Tag } =
       amqp_channel:subscribe(Channel, Sub, ConsumerPid),
   {ok, Tag}.
