-module(turtle_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

suite() ->
    [{timetrap, {seconds, 30}}].

init_per_group(basic, Config) ->
%%     {ok, _SaslApps} = application:ensure_all_started(sasl),
%%     dbg:tracer(),
%%     dbg:p(all, c),
%%     dbg:tpl(amqp_selective_consumer, handle_consume, '_', cx),
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

init_per_testcase(xfaulty_service, Config) ->
    dbg:tracer(),
    dbg:p(all, c),
    dbg:tpl(turtle_subscriber, '_', '_', cx),
    dbg:tpl(amqp_channel, cast, '_', cx),
    dbg:tpl(amqp_channel, call, '_', cx),
    Config;
init_per_testcase(send_recv, Config) ->
    exometer:delete([amqp_server, local_publisher, casts]),
    Config;
init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(xfaulty_service, _Config) ->
    dbg:stop_clear(),
    ok;
end_per_testcase(_Case, _Config) ->
    ok.

lifetime_group() ->
    [{lifetime, [shuffle], [
        start_stop
    ]}].

basic_group() ->
   [{basic, [],
     [
      send_recv,
      send_recv_confirm,
      rpc,
      kill_publisher,
      kill_service,
      kill_amqp_client,
      faulty_service,
      bulk
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
    ct:sleep(400),
    #{ connection_count := 1 } = turtle_janitor:status(),
    ok = application:stop(turtle).

send_recv_confirm(_Config) ->
    CastsB = get_casts(),
    MsgsB = get_msgs(),
    X = <<"send_recv_exchange_c">>,
    Q = <<"send_recv_queue_c">>,

    ct:log("Add a subscriber service, consuming on Q"),
    Self = self(),
    F = fun(Key, ContentType, Payload, _State) ->
        Self ! {Key, ContentType, Payload},
        ack
    end,
    {ok, _ServicePid} = turtle_service:start_link(
        #{
            name => local_service,
            connection => amqp_server,
            function => F,
            declarations =>
               [#'exchange.declare' { exchange = X },
                #'queue.declare' { queue = Q, durable = true },
                #'queue.bind' { queue = Q, exchange = X, routing_key = Q }],
            subscriber_count => 3,
            prefetch_count => 10,
            consume_queue => Q
        }),

    ct:log("Start a new publisher process"),
    {ok, _Pid} = turtle_publisher:start_link(local_publisher, amqp_server,
               [#'exchange.declare' { exchange = X },
                #'queue.declare' { queue = Q, durable = true },
                #'queue.bind' { queue = Q, exchange = X, routing_key = Q }],
                #{ confirms => true} ),

    ct:log("Await the start of the publisher"),
    gproc:await({n,l,{turtle,publisher,local_publisher}}, 300),
    ct:log("Await the start of the service"),
    gproc:await({n,l,{turtle,service_channel,local_service}}, 300),

    %% One publisher, 3 workers
    #{ channel_count := 4 } = turtle_janitor:status(),

    ct:log("Publish a message on the channel"),
    turtle:publish(local_publisher, X, Q, <<"text/plain">>, <<"The turtle and the hare">>),
    {ack, _Time} = turtle:publish_sync(local_publisher, X, Q, <<"text/plain">>, <<"The hare and the turtle">>),

    receive
        {Q, <<"text/plain">>, <<"The turtle and the hare">>} ->
            ok
    after 400 ->
        ct:fail(subscription_timeout)
    end,

    receive
        {Q, <<"text/plain">>, <<"The hare and the turtle">>} ->
            ok
    after 400 ->
        ct:fail(subscription_timeout)
    end,

    %% Wait a bit for stability of the underlying counts
    ct:sleep(20),

    CastsA = get_casts(),
    1 = CastsA - CastsB,
    MsgsA = get_msgs(),
    2 = MsgsA - MsgsB,
    ok.

rpc(_Config) ->
    X = <<"rpc_exchange">>,
    Q = <<"rpc_queue">>,

    ct:log("Add a subscriber service, echoing incoming messages"),
    F = fun(_Key, ContentType, Payload, _State) ->
        {reply, ContentType, Payload}
    end,
    HI = fun(_Info, State) -> {ok, State} end,
    TConf = #{
        name => local_service,
        connection => amqp_server,
        function => F,
        handle_info => HI,
        init_state => #{},
        declarations =>
        [#'exchange.declare' { exchange = X },
            #'queue.declare' { queue = Q },
            #'queue.bind' { queue = Q, exchange = X, routing_key = Q }],
        subscriber_count => 3,
        prefetch_count => 10,
        consume_queue => Q
    },
    ct:log("Creating Child Specs"),
    _ChildSpecs = turtle_service:child_spec(TConf),
    {ok, _ServicePid} = turtle_service:start_link(TConf),

    ct:log("Start a new publisher process"),
    {ok, _Pid} = turtle_publisher:start_link(local_publisher, amqp_server, [],
               #{ confirms => true, rpc => enable }),

    ct:log("Await the start of the publisher"),
    gproc:await({n,l,{turtle,publisher,local_publisher}}, 300),
    ct:log("Await the start of the service"),
    gproc:await({n,l,{turtle,service_channel,local_service}}, 300),

    %% One publisher, 3 workers, the earlier tests should be done
    #{ channel_count := 4 } = turtle_janitor:status(),

    ct:log("Run a single call"),
    {ok, _, <<"text/plain">>, <<"Hello world!">>} =
        turtle:rpc_sync(local_publisher, X, Q, <<"text/plain">>, <<"Hello world!">>),

    ct:log("Run another single call"),
    {ok, _, <<"text/plain">>, <<"Hello world! (2)">>} =
        turtle:rpc_sync(local_publisher, X, Q, <<"text/plain">>, <<"Hello world! (2)">>),

    run_many_rpc(5, 1000),
    ok.

bulk_loop(_Key, _CType, _Payload, T, State) ->
    ct:log("bulk_loop(~p)", [{_Key, _CType, _Payload, T, State}]),
    case State of
        #{ tags := [] } = S ->
            TRef = timer:send_after(75, self(), batch_timeout),
            {[], S#{ timer := TRef, tags := [T] }};
        #{ tags := Ts } = S ->
            {[], S#{ tags := [T | Ts]}}
    end.

bulk_handle_info(batch_timeout, #{ recipient := TestPid, tags := Tags } = S) ->
    ct:log("bulk_handle_info(~p)", [{batch_timeout, S}]),
    TestPid ! length(Tags),
    {[{ack, T} || T <- Tags], S#{ tags := [], timer := undefined }}.

bulk(_Config) ->
    X = <<"send_recv_exchange">>,
    Q = <<"send_recv_queue">>,

    ct:log("Add a bulk subscriber service, consuming on Q"),
    Self = self(),
    TConf = #{
        name => local_service,
        connection => amqp_server,
        function => fun bulk_loop/5,
        handle_info => fun bulk_handle_info/2,
        init_state => #{ recipient => Self, timer => undefined, tags => [] },
        declarations =>
        [#'exchange.declare' { exchange = X },
            #'queue.declare' { queue = Q },
            #'queue.bind' { queue = Q, exchange = X, routing_key = Q }],
        subscriber_count => 1,
        prefetch_count => 5,
        consume_queue => Q,
        mode => bulk
    },
    ct:log("Creating Child Specs"),
    _ChildSpecs = turtle_service:child_spec(TConf),
    {ok, _ServicePid} = turtle_service:start_link(TConf),
    ct:log("Start a new publisher process"),
    {ok, _Pid} = turtle_publisher:start_link(local_publisher, amqp_server,
               [#'exchange.declare' { exchange = X },
                #'queue.declare' { queue = Q },
                #'queue.bind' { queue = Q, exchange = X, routing_key = Q }]),

    ct:log("Await the start of the publisher"),
    gproc:await({n,l,{turtle,publisher,local_publisher}}, 300),
    ct:log("Await the start of the service"),
    gproc:await({n,l,{turtle,service_channel,local_service}}, 300),

    %% One publisher, 1 worker, the earlier tests should be done
    #{ channel_count := 2 } = turtle_janitor:status(),

    ct:log("Publish a message on the channel"),
    [turtle:publish(local_publisher, X, Q, <<"text/plain">>, <<"The turtle and the hare">>)
      || _ <- lists:seq(1, 10)],

    receive
        5 -> ok
    after 500 ->
        ct:fail(subscription_timeout)
    end,

    %% Next batch
     receive
       5 -> ok
    after 500 ->
        ct:fail(subscription_timeout)
    end,
    ok.

send_recv(_Config) ->
    X = <<"send_recv_exchange">>,
    Q = <<"send_recv_queue">>,

    ct:log("Add a subscriber service, consuming on Q"),
    Self = self(),
    F = fun(Key, ContentType, Payload, _State) ->
        Self ! {Key, ContentType, Payload},
        ack
    end,
    {ok, _ServicePid} = turtle_service:start_link(
        #{
            name => local_service,
            connection => amqp_server,
            function => F,
            declarations =>
               [#'exchange.declare' { exchange = X },
                #'queue.declare' { queue = Q },
                #'queue.bind' { queue = Q, exchange = X, routing_key = Q }],
            subscriber_count => 3,
            prefetch_count => 10,
            consume_queue => Q
        }),

    ct:log("Start a new publisher process"),
    {ok, _Pid} = turtle_publisher:start_link(local_publisher, amqp_server,
               [#'exchange.declare' { exchange = X },
                #'queue.declare' { queue = Q },
                #'queue.bind' { queue = Q, exchange = X, routing_key = Q }]),

    ct:log("Await the start of the publisher"),
    turtle:await(publisher, local_publisher, 300),
    
    ct:log("Test the i/0 command"),
    #{
       publishers := #{
         local_publisher := P }} = turtle:i(),
    true = is_pid(P),
    ct:log("Await the start of the service"),
    gproc:await({n,l,{turtle,service_channel,local_service}}, 300),

    ct:log("Publish a message on the channel"),
    turtle:publish(local_publisher, X, Q, <<"text/plain">>, <<"The turtle and the hare">>),
    ok = turtle:publish_sync(local_publisher, X, Q, <<"text/plain">>, <<"The hare and the turtle">>),

    receive
        {Q, <<"text/plain">>, <<"The turtle and the hare">>} ->
            ok
    after 400 ->
        ct:fail(subscription_timeout)
    end,

    receive
        {Q, <<"text/plain">>, <<"The hare and the turtle">>} ->
            ok
    after 400 ->
        ct:fail(subscription_timeout)
    end,

    %% Wait a bit for stability of the underlying counts
    ct:sleep(20),

    {ok, PVals} = exometer:get_value([amqp_server, local_publisher, casts]),
    1 = proplists:get_value(one, PVals),
    {ok, SVals} = exometer:get_value([amqp_server, local_service, msgs]),
    ct:log("Latency structure: ~p", [SVals]),
    2 = proplists:get_value(one, SVals),
    {ok, _SLvals} = exometer:get_value([amqp_server, local_service, latency]),
    ok.

faulty_service(_Config) ->
    X = <<"send_recv_exchange">>,
    Q = <<"send_recv_queue">>,

    ct:log("Add a faulty subscriber, consuming on Q"),
    Self = self(),
    F = fun
        (_Key, _ContentType, <<"Fail">>, _State) -> error(fail);
        (Key, ContentType, Payload, _State) ->
            Self ! {Key, ContentType, Payload},
            ack
    end,

    {ok, _ServicePid} = turtle_service:start_link(
        #{
            name => local_service,
            connection => amqp_server,
            function => F,
            declarations =>
               [#'exchange.declare' { exchange = X },
                #'queue.declare' { queue = Q },
                #'queue.bind' { queue = Q, exchange = X, routing_key = Q }],
            subscriber_count => 3,
            prefetch_count => 5,
            consume_queue => Q
        }),

    ct:log("Start a new publisher process"),
    {ok, _Pid} = turtle_publisher:start_link(local_publisher, amqp_server,
               [#'exchange.declare' { exchange = X },
                #'queue.declare' { queue = Q },
                #'queue.bind' { queue = Q, exchange = X, routing_key = Q }]),

    ct:log("Await the start of the publisher"),
    gproc:await({n,l,{turtle,publisher,local_publisher}}, 300),
    ct:log("Await the start of the service"),
    gproc:await({n,l,{turtle,service_channel,local_service}}, 300),

    %% One publisher, 1 worker, the earlier tests should be done
    #{ channel_count := 4 } = turtle_janitor:status(),

    ct:log("Publish some messages on the channel:"),
    [
        turtle:publish(local_publisher, X, Q, <<"text/plain">>, <<"Fail">>)
        || _ <- lists:seq(1, 3)],
    [
        turtle:publish(local_publisher, X, Q, <<"text/plain">>, <<"The turtle and the hare">>)
        || _ <- lists:seq(1, 7)],

    ok = faulty_receive(7),

    ct:sleep(200),

    %% After a while, the system should have remedied itself
    #{ channel_count := 4 } = turtle_janitor:status(),

    ok.

kill_service(_Config) ->
    X = <<"send_recv_exchange">>,
    Q = <<"send_recv_queue">>,

    ct:log("Add a subscriber service, consuming on Q"),
    Self = self(),
    F = fun(Key, ContentType, Payload, _State) ->
        Self ! {Key, ContentType, Payload},
        ack
    end,
    {ok, _ServicePid} = turtle_service:start_link(
        #{
            name => local_service,
            connection => amqp_server,
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

    %% After a while, the system should have remedied itself
    #{ channel_count := 3 } = turtle_janitor:status(),

    ct:log("Flush the queue"),
    flush(),
    ct:log("Kill the connection, check that the service goes away"),
    _ConnPid = gproc:where({n,l,{turtle,connection, amqp_server}}),
    MgrPid = gproc:where({n,l,{turtle,service_channel,local_service}}),
    MRef = erlang:monitor(process, MgrPid),
    true = (MgrPid /= undefined),
    turtle_conn:close(amqp_server),
    receive
        {'DOWN', MRef, process, _, Reason} ->
            ct:log("Service Chan exit: ~p", [Reason]),
            ok
    after 400 ->
        ct:fail(service_did_not_exit)
    end,

    ct:log("Service restarted, now try using it!"),
    ct:sleep(200),
    %% After a while, the system should have remedied itself
    #{ channel_count := 3 } = turtle_janitor:status(),

    ct:log("Start a new publisher process"),
    {ok, _Pid} = turtle_publisher:start_link(local_publisher, amqp_server,
               [#'exchange.declare' { exchange = X },
                #'queue.declare' { queue = Q },
                #'queue.bind' { queue = Q, exchange = X, routing_key = Q }]),

    ct:log("Await the start of the publisher"),
    gproc:await({n,l,{turtle,publisher,local_publisher}}, 300),
    ct:log("Check that the process got restarted and re-registered itself"),
    MgrPid2 = gproc:await({n,l,{turtle,service_channel,local_service}}, 300),
    true = MgrPid /= MgrPid2,

    ct:log("Publish a message on the channel"),
    M = term_to_binary({msg, rand:uniform(16#FFFF)}),
    turtle:publish(local_publisher, X, Q, <<"text/plain">>, M),
    receive
        {Q, <<"text/plain">>, M} ->
            ok;
        {Q, <<"text/plain">>, _M2} ->
            ct:log("Received other message"),
            ok;
        Msg2 ->
            ct:fail({unexpected_msg, Msg2})
    after 400 ->
        ct:fail(subscription_timeout)
    end.

kill_amqp_client(_Config) ->
    X = <<"send_recv_exchange">>,
    Q = <<"send_recv_queue">>,

    ct:log("Add a subscriber service, consuming on Q"),
    Self = self(),
    F = fun(Key, ContentType, Payload, _State) ->
        Self ! {Key, ContentType, Payload},
        ack
    end,
    {ok, _ServicePid} = turtle_service:start_link(
        #{
            name => local_service,
            connection => amqp_server,
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

    %% After a while, the system should have remedied itself
    #{ channel_count := 3,
       connections := [#{ connection := ConnPid }] } = turtle_janitor:status(),

    ct:log("Flush the queue"),
    flush(),

    ct:log("Invoke a heartbeat timeout"),
    ConnPid ! heartbeat_timeout,

    ct:log("Service restarted, now try using it!"),
    ct:sleep(200),
    %% After a while, the system should have remedied itself
    #{ channel_count := 3 } = turtle_janitor:status(),

    ct:log("Start a new publisher process"),
    {ok, _Pid} = turtle_publisher:start_link(local_publisher, amqp_server,
               [#'exchange.declare' { exchange = X },
                #'queue.declare' { queue = Q },
                #'queue.bind' { queue = Q, exchange = X, routing_key = Q }]),

    ct:log("Await the start of the publisher"),
    gproc:await({n,l,{turtle,publisher,local_publisher}}, 300),
    ct:log("Check that the process got restarted and re-registered itself"),
    MgrPid2 = gproc:await({n,l,{turtle,service_channel,local_service}}, 300),

    ct:log("Publish a message on the channel"),
    M = term_to_binary({msg, rand:uniform(16#FFFF)}),
    turtle:publish(local_publisher, X, Q, <<"text/plain">>, M),
    receive
        {Q, <<"text/plain">>, M} ->
            ok;
        {Q, <<"text/plain">>, _M2} ->
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
    {ok, _Pid} = turtle_publisher:start_link(local_publisher, amqp_server, [
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
    _ConnPid = gproc:where({n,l,{turtle,connection,amqp_server}}),
    PublisherPid = gproc:where({n,l,{turtle,publisher,local_publisher}}),
    turtle_conn:close(amqp_server),
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

%% Obtain different exometer statistics.
get_casts() ->
    {ok, PVals} = exometer:get_value([amqp_server, local_publisher, casts]),
    proplists:get_value(one, PVals).

get_msgs() ->
    {ok, SVals} = exometer:get_value([amqp_server, local_service, msgs]),
    proplists:get_value(one, SVals).

run_many_rpc(Workers, K) ->
    F = fun() ->
         rpc_worker_loop(K),
         ct:log("Worker done"),
         ok
    end,
    Processes = [spawn_monitor(F) || _ <- lists:seq(1, Workers)],
    ct:log("Spawned workers"),
    await(Processes).

await([{_Pid, MRef} | Left]) ->
    receive
        {'DOWN', MRef, process, _, normal} ->
            await(Left);
        {'DOWN', MRef, process, _, Reason} ->
            {error, Reason}
    after 20000 ->
        {error, timeout}
    end;
await([]) -> ok.

rpc_worker_loop(0) -> ok;
rpc_worker_loop(K) ->
    X = <<"rpc_exchange">>,
    Q = <<"rpc_queue">>,

    I = rand:uniform(10000),
    {ok, _, <<"ctype">>, <<I:64/integer>>} =
        turtle:rpc_sync(local_publisher, X, Q, <<"ctype">>, <<I:64/integer>>),
    rpc_worker_loop(K-1).

faulty_receive(0) -> ok;
faulty_receive(N) ->
    receive
        {_, <<"text/plain">>, <<"The turtle and the hare">>} ->
            ct:pal("Got message for N=~B", [N]),
            faulty_receive(N-1)
    after 15*1000 ->
        ct:fail(subscription_timeout)
    end.
