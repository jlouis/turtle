%%%-------------------------------------------------------------------
%%% @author Jesper Louis Andersen <jesper.louis.andersen@gmail.com>
%%% @copyright (C) 2017, Jesper Louis Andersen
%%% @doc Maintain connections, channels and consumers
%%%
%%% @end
%%% Created : 14 Jul 2017 by Jesper Louis Andersen <jesper.louis.andersen@gmail.com>
%%%-------------------------------------------------------------------
-module(turtle_janitor).
-behaviour(gen_server).

%% API
-export([start_link/0]).
-export([open_channel/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, { bimap = #{} }).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

open_channel(Name) ->
    gen_server:call(?SERVER, {open_channel, Name}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    process_flag(trap_exit, true),
    {ok, #state{}}.


handle_call({open_channel, Name}, {Pid, _}, #state { bimap = BiMap } = State) ->
    case turtle_conn:open_channel(Name) of
        {ok, Channel} ->
            %% Hand out a channel to Pid
            MRef = erlang:monitor(process, Pid),
            {reply,
             {ok, Channel},
             State#state { bimap = bimap_put({channel, Channel}, MRef, BiMap) }};
        Err ->
            {reply, Err, State}
    end;
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', MRef, process, _Pid, _},
            #state { bimap = BiMap } = State) ->
    {Val, Cleaned} = bimap_take(MRef, BiMap),
    ok = cleanup(Val),
    {noreply, State#state { bimap = Cleaned }};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

cleanup({channel, Ch}) ->
    catch amqp_channel:close(Ch),
    ok;
cleanup({connection, Conn}) ->
    catch amqp_connection:close(Conn),
    ok;
cleanup(not_found) ->
    ok.

bimap_take(X, Map) ->
    case maps:take(X, Map) of
        error ->
            {not_found, Map};
        Val ->
            {Val, maps:remove(Val, Map)}
    end.

bimap_put(X, Y, Map) ->
    M1 = maps:put(X, Y, Map),
    M2 = maps:put(Y, X, M1),
    M2.

%% bimap_remove(X, Map) ->
%%     case maps:get(X, Map, '$$$') of
%%         '$$$' ->
%%             Map;
%%         Y ->
%%             M1 = maps:remove(X, Map),
%%             M2 = maps:remove(Y, Map),
%%             M2
%%     end.

