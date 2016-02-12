%%%-------------------------------------------------------------------
%% @doc Manage a turtle service.
%% These services listen on AMQP and processes data on an AMQP socket.
%% For each incoming message, it calls function which can then be used to
%% do work in the system.
%% @end
%%%-------------------------------------------------------------------
-module(turtle_service).

-behaviour(supervisor).

%% API
-export([start_link/1, child_spec/1]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================

%% @doc Start the service supervisor tree
%% For the definition of the `Conf' parameter, see {@link child_spec/1}
%% @end
start_link(#{ name := Name } = Conf) ->
    supervisor:start_link({via, gproc, {n,l,{turtle,service,Name}}}, ?MODULE, [Conf]).

%% @doc Generate a child specification for this supervisor
%% The Configuration is a map with the following keys:
%%
%% <dl>
%% <dt>name</dt>
%%   <dd>The name of the service (for gproc registration)</dd>
%% <dt>connection</dt>
%%   <dd>The turtle connection id to use</dd>
%% <dt>function</dt>
%%   <dd>The function to call when processing an event</dd>
%% <dt>handle_info</dt>
%%   <dd>The function to call when processing a message</dd>
%% <dt>init_state</dt>
%%   <dd>Initial state to use in the processing loop</dd>
%% <dt>declarations</dt>
%%   <dd>A list of declarations to execute against the AMQP server when starting up.
%%     The format is as you would normally cast to an AMQP channel.</dd>
%% <dt>subscriber_count</dt>
%%   <dd>Number of children to run in the subscriber pool</dd>
%% <dt>prefetch_count</dt>
%%   <dd>The prefetch setting on the channel for QoS and hiding network latency.</dd>
%% <dt>consume_queue</dt>
%%   <dd>The queue to consume from for this service.</dd>
%% <dt>passive</dt>
%%   <dd>true/false value - designates if we should force passive queue/exchange creation.</dd>
%% </dl>
child_spec(#{ name := Name } = Conf) ->
    validate_config(Conf),
    {Name, {?MODULE, start_link, [Conf]},
       permanent, infinity, supervisor, [?MODULE]}.

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% @private
%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([#{ name := Name } = Conf]) ->
    ChanMgr =
        {channel,
            {turtle_channel, start_link, [Conf]},
            permanent, 3000, worker, [turtle_channel]},
    Pool =
        {pool,
            {turtle_subscriber_pool, start_link, [Name]},
            permanent, infinity, supervisor, [turtle_subscriber_pool]},
    {ok, { { one_for_all, 5, 3600}, [ChanMgr, Pool]}}.
    
validate_config(#{
	name := N,
	connection := C,
	function := F,
	handle_info := HI,
	init_state := _IS,
	declarations := Decls,
	subscriber_count := SC,
	prefetch_count := PC,
	consume_queue := Q })
    when
      is_atom(N),
      is_atom(C),
      is_function(F, 4),
      is_function(HI, 2),
      is_list(Decls),
      is_integer(SC), SC > 0,
      is_integer(PC), PC >= 0,
      is_binary(Q) ->
      	ok.

