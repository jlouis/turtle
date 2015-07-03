%%% @doc Configuration management for the Turtle subsystem
%%% @end
%% @private
-module(turtle_config).

-export([read_params/0]).

read_params() ->
    {ok, EnvOrder} = application:get_env(turtle, gproc_env_order),
    gproc:get_env(l,turtle,connection_config, EnvOrder).
