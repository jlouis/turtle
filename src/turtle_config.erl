%%% @doc Configuration management for the Turtle subsystem
%%% @end
%% @private
-module(turtle_config).
-include_lib("amqp_client/include/amqp_client.hrl").

-export([read_params/0, conn_params/1]).

read_params() ->
    {ok, EnvOrder} = application:get_env(turtle, gproc_env_order),
    gproc:get_env(l,turtle,connection_config, EnvOrder).

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
