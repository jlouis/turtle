%% @doc conn_sup runs the main supervisor for a connection
%% There are two children: one running the connection itself, and one running
%% the supervisor which handles channel configuration.
%% @end
-module(turtle_conn_sup).

-behaviour(supervisor).

%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================

start_link(Configuration) ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, [Configuration]).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([Configuration]) ->
    ChannelSup =
        {channel_sup,
          {turtle_chan_sup, start_link, []},
          transient, infinity, supervisor, [turtle_chan_sup]},
    Conn = {connection,
        {turtle_conn, start_link, [Configuration]},
        permanent, 5000, worker, [turtle_conn]},
    {ok, { {one_for_all, 30, 3600}, [ChannelSup, Conn]} }.

%%====================================================================
%% Internal functions
%%====================================================================
