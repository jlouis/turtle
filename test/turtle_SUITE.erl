-module(turtle_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

suite() ->
    [{timetrap, {seconds, 15}}].
    
init_per_group(basic, Config) ->
%%     {ok, _SaslApps} = application:ensure_all_started(sasl),
%%     dbg:tracer(),
%%     dbg:p(all, c),
%%    dbg:tpl(amqp_selective_consumer, handle_consume, '_', cx),
%%     dbg:tpl(amqp_selective_consumer, deliver, 3, cx),
    {ok, _Apps} = application:ensure_all_started(turtle),
    Config;
init_per_group(_Group, Config) ->
    Config.

end_per_group(basic, _Config) ->
    ok = application:stop(turtle),
    dbg:stop_clear(),
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
   [{basic, [], [
       send_recv,
       kill_publisher,
       kill_service
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

    ct:log("Add a subscriber service, consuming on Q"),
    Self = self(),
    F = fun(Key, ContentType, Payload) ->
        Self ! {Key, ContentType, Payload},
        ack
    end,
    {ok, _ServicePid} = turtle_service:start_link(
        #{
            name => local_service,
            connection => local_test,
            function => F,
            declarations =>
               [#'exchange.declare' { exchange = X },
                #'queue.declare' { queue = Q },
                #'queue.bind' { queue = Q, exchange = X, routing_key = Q }],
            subscriber_count => 3,
            consume_queue => Q
        }),         

    ct:log("Start a new publisher process"),
    {ok, _Pid} = turtle_publisher:start_link(local_publisher, local_test, 
               [#'exchange.declare' { exchange = X },
                #'queue.declare' { queue = Q },
                #'queue.bind' { queue = Q, exchange = X, routing_key = Q }]),
    
    ct:log("Await the start of the publisher"),
    gproc:await({n,l,{turtle,publisher,local_publisher}}, 300),
    ct:log("Await the start of the service"),
    gproc:await({n,l,{turtle,service_channel,local_service}}, 300),

    ct:log("Publish a message on the channel"),
    turtle:publish(local_publisher, X, Q, <<"text/plain">>, <<"The turtle and the hare">>),
    receive
        {Q, <<"text/plain">>, <<"The turtle and the hare">>} ->
            ok
    after 400 ->
        ct:fail(subscription_timeout)
    end.
    
kill_service(_Config) ->
    X = <<"send_recv_exchange">>,
    Q = <<"send_recv_queue">>,

    ct:log("Add a subscriber service, consuming on Q"),
    Self = self(),
    F = fun(Key, ContentType, Payload) ->
        Self ! {Key, ContentType, Payload},
        ack
    end,
    {ok, _ServicePid} = turtle_service:start_link(
        #{
            name => local_service,
            connection => local_test,
            function => F,
            declarations =>
               [#'exchange.declare' { exchange = X },
                #'queue.declare' { queue = Q },
                #'queue.bind' { queue = Q, exchange = X, routing_key = Q }],
            subscriber_count => 3,
            consume_queue => Q
        }),
    ct:log("Await the start of the service"),
    gproc:await({n,l,{turtle,service_channel,local_service}}, 300),

    ct:log("Flush the queue"),
    flush(),         
    ct:log("Kill the connection, check that the service goes away"),
    ConnPid = gproc:where({n,l,{turtle,connection,local_test}}),
    ChanPid = gproc:where({n,l,{turtle,service_channel,local_service}}),
    MRef = erlang:monitor(process, ChanPid),
    true = (ChanPid /= undefined),
    turtle_conn:close(local_test),
    receive
        {'DOWN', MRef, process, _, Reason} ->
            ct:log("Service Chan exit: ~p", [Reason]),
            ok
    after 400 ->
        ct:fail(service_did_not_exit)
    end,
    
    ct:log("Service restarted, now try using it!"),

    ct:log("Start a new publisher process"),
    {ok, _Pid} = turtle_publisher:start_link(local_publisher, local_test, 
               [#'exchange.declare' { exchange = X },
                #'queue.declare' { queue = Q },
                #'queue.bind' { queue = Q, exchange = X, routing_key = Q }]),
    
    ct:log("Await the start of the publisher"),
    gproc:await({n,l,{turtle,publisher,local_publisher}}, 300),
    ct:log("Check that the process got restarted and re-registered itself"),
    ChanPid2 = gproc:await({n,l,{turtle,service_channel,local_service}}, 300),
    true = ChanPid /= ChanPid2,

    ct:log("Publish a message on the channel"),
    M = term_to_binary({msg, rand:uniform(16#FFFF)}),
    turtle:publish(local_publisher, X, Q, <<"text/plain">>, M),
    receive
        {Q, <<"text/plain">>, M} ->
            ok;
        {Q, <<"text/plain">>, M2} ->
            ct:log("Received other message"),
            ok;
        Msg2 ->
            ct:fail({unexpected_msg, Msg2})
    after 400 ->
        ct:fail(subscription_timeout)
    end.

kill_publisher(_Config) ->
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
    turtle_conn:close(local_test),
    receive
        {'EXIT', PublisherPid, Reason} ->
            ct:log("Publisher exit: ~p", [Reason]),
            ok;
        Msg ->
            ct:fail({unexpected_msg, Msg})
    after 400 ->
        ct:fail(publisher_did_not_exit)
    end,
    process_flag(trap_exit, false),
    ok.

%% INTERNAL FUNCTIONS
%% ------------------------------------------
flush() ->
    receive
       _M -> flush()
    after 30 ->
       ok
    end.
