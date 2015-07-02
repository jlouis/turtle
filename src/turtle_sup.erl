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
        {turtle_conn_sup, start_link, [Name, conn_params(Ps)]},
        permanent, infinity, supervisor, [turtle_conn_sup]}.

conn_params(Ps) ->
    #amqp_params_network {
        username = maps:get(username, Ps, <<"guest">>),
        password = maps:get(password, Ps, <<"guest">>),
        virtual_host = maps:get(virtual_host, Ps, <<"/">>),
        host = maps:get(host, Ps, "localhost"),
        port = maps:get(port, Ps, 5672),
        
        channel_max = maps:get(channel_max, Ps, 0),
        frame_max = maps:get(frame_max, Ps, 0),
        heartbeat = maps:get(heartbeat, Ps, 15)
        
        %% Not setting:
        %%  - ssl_options
        %%  - auth_mechanisms
        %%  - client_properties
    }.
