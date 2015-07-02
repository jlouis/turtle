-module(turtle).

-export([open_channel/1]).

%% @doc open_channel/1 opens a channel on a given connection
%% This function will return a channel on a given connection, bound in
%% the system.
%% @end
open_channel(Name) -> turtle_conn:open_channel(Name).
