%%%-------------------------------------------------------------------
%% @doc Manage a pool of subscribers
%% @end
%%%-------------------------------------------------------------------
%% @private
-module(turtle_subscriber_pool).
-behaviour(supervisor).

%% API
-export([start_link/1]).
-export([add_subscriber/4]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================

start_link(Name) ->
    supervisor:start_link({via, gproc, {n,l,{turtle,service_pool,Name}}}, ?MODULE, []).

add_subscriber(Pool, Channel, Fun, Queue) ->
    supervisor:start_child(Pool, [Channel, Fun, Queue]).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([]) ->
    SubChild =
        {subscriber,
            {turtle_subscriber, start_link, []},
            permanent, 15*1000, worker, [turtle_subscriber]},

    {ok, { { simple_one_for_one, 100, 3600}, [SubChild]}}.

%%====================================================================
%% Internal functions
%%====================================================================
