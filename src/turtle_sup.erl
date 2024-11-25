%%%-------------------------------------------------------------------
%% @doc Turtle top level supervisor
%% @end
%%%-------------------------------------------------------------------
%% @private
-module('turtle_sup').

-behaviour(supervisor).

-include_lib("amqp_client/include/amqp_client.hrl").

%% API
-export([
        add_connection/1,
        start_link/0,
        stop_connection/1
        ]).

%test
-export([
        remove_application_config/1
        ]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

add_connection(Configuration) ->
	Spec = conn_sup(Configuration),
    add_application_config(Configuration),
    supervisor:start_child(turtle_sup,Spec).

stop_connection(Configuration) ->
    Name = maps:get(conn_name, Configuration),
    remove_application_config(Name),
    supervisor:terminate_child(turtle_sup,Name),
    supervisor:delete_child(turtle_sup,Name).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([]) ->
    Janitor = #{ id => turtle_janitor,
                 start => {turtle_janitor, start_link, []},
                 restart => permanent,
                 shutdown => 5000,
                 type => worker,
                 modules => [turtle_janitor]
               },
    Connectors = configure_connectors(),
    {ok, { {one_for_one, 5, 3600}, [Janitor | Connectors]} }.

%%====================================================================
%% Internal functions
%%====================================================================
configure_connectors() ->
    ParamSet = turtle_config:read_params(),
    [conn_sup(Params) || Params <- ParamSet].

conn_sup(#{conn_name := Name} = Ps) ->
    #{ id => Name,
       start => {turtle_conn, start_link, [Name, Ps]},
       restart => permanent,
       shutdown => 5000,
       type => worker
     }.

add_application_config(Configuration) ->
	CurrentConfig = application:get_env(turtle, connection_config, []),
	NewConfig = CurrentConfig ++ [Configuration],
	application:set_env(turtle,connection_config,NewConfig).

remove_application_config(ConfigName) ->
	CurrentConfig = application:get_env(turtle, connection_config, []),
    ConfigMap = list_of_maps_to_map(CurrentConfig,conn_name),
	NewConfigMap = maps:remove(ConfigName,ConfigMap),
	NewConfig = maps:values(NewConfigMap),
	application:set_env(turtle,connection_config,NewConfig).

list_of_maps_to_map(ListOfMaps, Key) ->
    list_of_maps_to_map(ListOfMaps, Key, #{}).

list_of_maps_to_map([],_Key,MapAcc) ->
    MapAcc;
list_of_maps_to_map(ListOfMaps,Key,MapAcc) ->
    KeyValue = maps:get(Key,hd(ListOfMaps)),
    NewMap = hd(ListOfMaps),
    MapAcc@1 = maps:put(KeyValue,NewMap,MapAcc),
    list_of_maps_to_map(tl(ListOfMaps),Key,MapAcc@1).
