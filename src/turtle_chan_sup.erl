%% @doc conn_sup runs the main supervisor for a connection
%% There are two children: one running the connection itself, and one running
%% the supervisor which handles channel configuration.
%% @end
%% @private
-module(turtle_chan_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
%% @private
init([]) ->
   ChanWorker = {channel_worker,
        {turtle_channel_worker, start_link, []},
        transient, 5000, worker, [turtle_channel_worker]},
    {ok, { {simple_one_for_one, 10, 600}, [ChanWorker]} }.

%%====================================================================
%% Internal functions
%%====================================================================
