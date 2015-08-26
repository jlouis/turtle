%%% @doc Configuration management for the Turtle subsystem
%%% @end
%% @private
-module(turtle_config).
-include_lib("amqp_client/include/amqp_client.hrl").

-export([read_params/0, conn_params/1]).

read_params() ->
    {ok, Conf} = application:get_env(turtle, connection_config),
    Conf.

conn_params(Ps) ->
    #amqp_params_network {
        username = username(Ps),
        password = password(Ps),
        virtual_host = virtual_host(Ps),

        channel_max = maps:get(channel_max, Ps, 0),
        frame_max = maps:get(frame_max, Ps, 0),
        heartbeat = maps:get(heartbeat, Ps, 15)

        %% Not setting:
        %%  - ssl_options
        %%  - auth_mechanisms
        %%  - client_properties
    }.


username(#{ username := U }) -> list_to_binary(U).
password(#{ password := PW }) -> list_to_binary(PW).
virtual_host(#{ virtual_host := VH }) -> list_to_binary(VH).
