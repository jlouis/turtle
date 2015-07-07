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
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================

%% @doc Start the service supervisor tree
%% This tree is ready for OTP linkage
%% @end
start_link(#{ name := Name } = Conf) ->
    supervisor:start_link({via, gproc, {n,l,{turtle,service,Name}}}, ?MODULE, [Conf]).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([#{ name := Name } = Conf]) ->
    ChanMgr =
        {channel,
            {turtle_channel, start_link, [Conf]},
            permanent, 3000, worker, [turtle_channel]},
    Pool =
        {pool,
            {turtle_subscriber_pool, start_link, [Name]},
            transient, infinity, supervisor, [turtle_subscriber_pool]},
    {ok, { { one_for_all, 5, 3600}, [Pool, ChanMgr]}}.