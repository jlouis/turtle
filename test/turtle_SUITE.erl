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
         
    ct:log("Kill the connection, check that the service goes away"),
    ConnPid = gproc:where({n,l,{turtle,connection,local_test}}),
    ChanPid = gproc:where({n,l,{turtle,service_channel,local_service}}),
    MRef = erlang:monitor(process, ChanPid),
    true = (ChanPid /= undefined),
    exit(ConnPid, s_dieinafire),
    receive
        {'DOWN', MRef, process, _, Reason} ->
            ct:log("Service Chan exit: ~p", [Reason]),
            ok;
        Msg ->
            ct:fail({unexpected_msg, Msg})
    after 400 ->
        ct:fail(service_did_not_exit)
    end,
    
    ct:log("Check that the process got restarted and re-registered itself"),
    ChanPid2 = gproc:await({n,l,{turtle,service_channel,local_service}}, 300),
    true = ChanPid /= ChanPid2,
    ok.

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
    exit(ConnPid, dieinafire),
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
