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
        host = host(Ps),
        port = port(Ps),

        channel_max = maps:get(channel_max, Ps, 0),
        frame_max = maps:get(frame_max, Ps, 0),
        heartbeat = maps:get(heartbeat, Ps, 15)

        %% Not setting:
        %%  - ssl_options
        %%  - auth_mechanisms
        %%  - client_properties
    }.


username(#{ username := U }) -> list_to_binary(env(U)).
password(#{ password := PW }) -> list_to_binary(env(PW)).
virtual_host(#{ virtual_host := VH }) -> list_to_binary(env(VH)).
host(#{ host := H }) -> env(H).
port(#{ port := P }) -> integerize(env(P)).

integerize(I) when is_integer(I) -> I;
integerize(Str) when is_list(Str) -> list_to_integer(Str).

env({env, E, Default}) -> os:getenv(E, Default);
env(Val) -> Val.