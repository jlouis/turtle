%%% @doc Configuration management for the Turtle subsystem
%%% @end
%% @private
-module(turtle_config).
-include_lib("amqp_client/include/amqp_client.hrl").

% Query and Format API
-export([read_params/0, conn_params/1]).

% Validation API
-export([validate_conn_name/1]).

-spec read_params() -> [map()].
read_params() ->
    {ok, Conf} = application:get_env(turtle, connection_config),
    Conf.

-spec conn_params(map()) -> term(). % @todo fix this typespec
conn_params(Ps) ->
    Params = case maps:get(ssl_options,Ps,false) of
        false -> 
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
            };
        _ ->
            #amqp_params_network {
                username = username(Ps),
                password = password(Ps),
                virtual_host = virtual_host(Ps),
                ssl_options = maps:get(ssl_options,Ps,[]),

                channel_max = maps:get(channel_max, Ps, 0),
                frame_max = maps:get(frame_max, Ps, 0),
                heartbeat = maps:get(heartbeat, Ps, 15)

                %% Not setting:
                %%  - auth_mechanisms
                %%  - client_properties
            }
    end,
    io:format("Params: ~p~n",[Params]),
    Params.



username(#{ username := U }) -> list_to_binary(U).
password(#{ password := PW }) -> list_to_binary(PW).
virtual_host(#{ virtual_host := VH }) -> list_to_binary(VH).
ssl_options(#{ ssl_options := SO }) -> SO.


-spec validate_conn_name(term()) -> ok | unknown_conn_name.
validate_conn_name(Name) ->
    ConfigList = application:get_env(turtle, connection_config, []),
    validate_conn_name(Name, ConfigList).

validate_conn_name(_, []) ->
    unknown_conn_name;
validate_conn_name(Name, [#{ conn_name := Name } | _]) ->
    ok;
validate_conn_name(Name, [_ | ConfigList]) ->
    validate_conn_name(Name, ConfigList).
