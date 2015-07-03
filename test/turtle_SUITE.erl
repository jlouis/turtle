-module(turtle_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

suite() ->
    [{timetrap, {seconds, 15}}].
    
init_per_group(basic, Config) ->
    {ok, _Apps} = application:ensure_all_started(turtle),
    Config;
init_per_group(_Group, Config) ->
    Config.

end_per_group(basic, _Config) ->
    ok = application:stop(turtle),
    ok;
end_per_group(_Group, _Config) ->
    ok.

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_Case, _Config) ->
    ok.

lifetime_group() ->
    [{lifetime, [shuffle], [
        start_stop
    ]}].

basic_group() ->
   [{basic, [shuffle], [
       send_recv,
       kill_connection
   ]}].

groups() ->
    lists:append([
      lifetime_group(),
      basic_group()
    ]).

all() -> [
    {group, lifetime},
    {group, basic}
].

%% TESTS
%% ------------------------------------------
start_stop(_Config) ->
    {ok, _Apps} = application:ensure_all_started(turtle),
    ct:sleep(200),
    ok = application:stop(turtle).

send_recv(_Config) ->
    X = <<"send_recv_exchange">>,
    Q = <<"send_recv_queue">>,

    ct:log("Open new channel to RabbitMQ"),
    {ok, Ch} = turtle:open_channel(local_test),
    ct:log("Declare a standard queue on which to do stuff"),
    ok = turtle:declare(Ch, [
        #'exchange.declare' { exchange = X },
        #'queue.declare' { queue = Q },
        #'queue.bind' {
            queue = Q,
            exchange = X,
            routing_key = Q
        }]),

    ct:log("Add a subscriber to the newly declared queue"),
    Self = self(),
    F = fun(Key, ContentType, Payload) ->
        Self ! {Key, ContentType, Payload},
        ack
    end,
    {ok, PoolPid} = turtle_subscriber_pool:start_link(),
    {ok, SubPid} =  turtle_subscriber_pool:add_subscriber(Ch, F, Q),
    
    ct:log("Start a new publisher process"),
    {ok, _Pid} = turtle_publisher:start_link(local_publisher, local_test, [
        #'exchange.declare' { exchange = X },
        #'queue.declare' { queue = Q },
        #'queue.bind' {
            queue = Q,
            exchange = X,
            routing_key = Q
        }]),
    gproc:await({n,l,{turtle,publisher,local_publisher}}, 300),

    ct:log("Publish a message on the channel"),
    turtle:publish(local_publisher, X, Q, <<"text/plain">>, <<"The turtle and the hare">>),
    receive
        {Q, <<"text/plain">>, <<"The turtle and the hare">>} ->
            ok
    after 40 ->
        ct:fail(subscription_timeout)
    end.
    
kill_connection(_Config) ->
    X = <<"send_recv_exchange">>,
    Q = <<"send_recv_queue">>,

    ct:log("Start the publisher on the connection"),
    {ok, _Pid} = turtle_publisher:start_link(local_publisher, local_test, [
        #'exchange.declare' { exchange = X },
        #'queue.declare' { queue = Q },
        #'queue.bind' {
            queue = Q,
            exchange = X,
            routing_key = Q
        }]),
    gproc:await({n,l,{turtle,publisher,local_publisher}}, 300),
    
    ct:log("Kill the connection, check that the publisher goes away"),
    process_flag(trap_exit, true),
    ConnPid = gproc:where({n,l,{turtle,connection,local_test}}),
    PublisherPid = gproc:where({n,l,{turtle,publisher,local_publisher}}),
    exit(ConnPid, dieinafire),
    receive
        {'EXIT', PublisherPid, Reason} ->
            ct:log("Publisher exit: ~p", [Reason]),
            ok;
        Msg ->
            ct:fail({unexpected_msg, Msg})
    after 40 ->
        ct:fail(publisher_did_not_exit)
    end,
    process_flag(trap_exit, false),
    ok.

%% INTERNAL FUNCTIONS
%% ------------------------------------------
