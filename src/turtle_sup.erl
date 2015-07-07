%%%-------------------------------------------------------------------
%% @doc Turtle top level supervisor
%% @end
%%%-------------------------------------------------------------------
%% @private
-module('turtle_sup').

-behaviour(supervisor).

-include_lib("amqp_client/include/amqp_client.hrl").

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
init([]) ->
    Connectors = configure_connectors(),
    {ok, { {one_for_one, 5, 3600}, Connectors} }.

%%====================================================================
%% Internal functions
%%====================================================================
configure_connectors() ->
    ParamSet = turtle_config:read_params(),
    [conn_sup(Params) || Params <- ParamSet].

conn_sup(#{ conn_name := Name } = Ps) ->
    {Name,
        {turtle_conn, start_link, [Name, turtle_config:conn_params(Ps)]},
        permanent, infinity, supervisor, [turtle_conn_sup]}.
