-module(turtle_conn).
-behaviour(gen_server).

%% Lifetime
-export([
	start_link/1
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-record(state, {}).

%% LIFETIME MAINTENANCE
%% ----------------------------------------------------------
start_link(Configuration) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Configuration], []).
	
%% CALLBACKS
%% -------------------------------------------------------------------

%% @private
init([_Configuration]) ->
    {ok, #state {}}.

%% @private
handle_call(Call, From, State) ->
    lager:warning("Unknown call from ~p: ~p", [From, Call]),
    {reply, {error, unknown_call}, State}.

%% @private
handle_cast(Cast, State) ->
    lager:warning("Unknown cast: ~p", [Cast]),
    {noreply, State}.

%% @private
handle_info(_, State) ->
    {noreply, State}.

%% @private
terminate(_Reason, _State) ->
    ok.

%% @private
code_change(_, State, _) ->
    {ok, State}.

%%
%% INTERNAL FUNCTIONS
%%
