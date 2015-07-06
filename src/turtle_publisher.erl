%%% @doc The publisher is a helper for publishing messages on a channel
%%% @end
-module(turtle_publisher).
-behaviour(gen_server).
-include_lib("amqp_client/include/amqp_client.hrl").

%% Lifetime
-export([
	start_link/3
]).

%% API
-export([
	publish/6
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-record(state, {
	channel,
	conn_ref
 }).

%% LIFETIME MAINTENANCE
%% ----------------------------------------------------------

%% @doc Start a new publication worker
%% Provides an OTP gen_server for supervisor linkage
%% @end
start_link(Name, Connection, Declarations) ->
    gen_server:start_link(?MODULE, [Name, Connection, Declarations], []).

%% @doc publish a message asynchronously to RabbitMQ
%% The specification is that you have to provide all parameters, because experience
%% has shown that you end up having to tweak these things quite a lot in practice.
%% Hence we provide the full kind of messaging, rather than a subset
%% @end
publish(Publisher, Exch, Key, ContentType, Payload, Opts) ->
    Pub = #'basic.publish' {
        exchange = Exch,
        routing_key = Key
    },
    Props = properties(ContentType, Opts),
    Pid = gproc:where({n,l,{turtle,publisher,Publisher}}),
    gen_server:cast(Pid, {publish, Pub, Props, Payload}).

%% CALLBACKS
%% -------------------------------------------------------------------

%% @private
init([Name, ConnName, Declarations]) ->
    %% Initialize the system in the {initializing,...} state and await the presence of
    %% a connection under the given name without blocking the process. We replace
    %% the state with a #state{} record once that happens (see handle_info/2)
    Ref = gproc:nb_wait({n,l,{turtle,connection,ConnName}}),
    {ok, {initializing, Name, Ref, ConnName, Declarations}}.

%% @private
handle_call(Call, From, State) ->
    lager:warning("Unknown call from ~p: ~p", [From, Call]),
    {reply, {error, unknown_call}, State}.

%% @private
handle_cast(Pub, {initializing, _, _, _, _} = Init) ->
    %% Messages cast to an initializing publisher are thrown away, but it shouldn't
    %% happen, so we log them
    lager:warning("Publish while initializing: ~p", [Pub]),
    {noreply, Init};
handle_cast({publish, Pub, Props, Payload}, #state { channel = Ch } = State) ->
    ok = amqp_channel:cast(Ch, Pub, #amqp_msg { props = Props, payload = Payload }),
    {noreply, State};
handle_cast(Cast, State) ->
    lager:warning("Unknown cast: ~p", [Cast]),
    {noreply, State}.

%% @private
handle_info({gproc, Ref, registered, {_, Pid, _}}, {initializing, N, Ref, CName, Decls}) ->
    {ok, Channel} = turtle:open_channel(CName),
    ok = turtle:declare(Channel, Decls),
    MRef = erlang:monitor(process, Pid),
    reg(N),
    {noreply, #state { channel = Channel, conn_ref = MRef}};
handle_info({'DOWN', MRef, process, _, Reason}, #state { conn_ref = MRef } = State) ->
    {stop, {error, {connection_down, Reason}}, State};
handle_info(Info, State) ->
    lager:warning("Received unknown info msg: ~p", [Info]),
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
reg(Name) ->
    true = gproc:reg({n,l,{turtle,publisher, Name}}).

%% Compute the properties of an AMQP message
properties(ContentType, #{ delivery_mode := persistent }) ->
    #'P_basic' { content_type = ContentType, delivery_mode = 2 };
properties(ContentType, #{ delivery_mode := ephermeral }) ->
    #'P_basic' { content_type = ContentType }.
