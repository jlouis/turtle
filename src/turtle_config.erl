%% @private
-module(turtle_config).

-export([read_params/0]).

read_params() -> [#{ conn_name => local_test }].
