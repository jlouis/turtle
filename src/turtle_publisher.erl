%%% @doc The publisher is a helper for publishing messages on a channel
%%% @end
-module(turtle_publisher).
-behaviour(gen_server).
-include_lib("amqp_client/include/amqp_client.hrl").

%% Lifetime
-export([
	start_link/3, start_link/4
]).

%% API
-export([
	publish/6,
	publish_sync/6
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
         conn_name,
         name,
	channel,
	conn_ref,
	confirms,
	unacked = gb_trees:empty()
 }).

-define(DEFAULT_OPTIONS,
    #{
        declarations => [],
        confirms => false
    }).

%% LIFETIME MAINTENANCE
%% ----------------------------------------------------------

%% @doc Start a new publication worker
%% Provides an OTP gen_server for supervisor linkage. The `Name' is the name of
%% this publisher (which it registers itself in gproc as). The `Connection' is the turtle-name
%% for the connection, i.e., `amqp_server'. Finally, `Declarations' is a declaration list to
%% be executed against AMQP when setting up.
%% @end
start_link(Name, Connection, Declarations) ->
    Options = maps:merge(?DEFAULT_OPTIONS, #{ declarations => Declarations}),
    gen_server:start_link(?MODULE, [Name, Connection, Options], []).

%% @doc start_link/4 starts a publisher with options
%% This variant of start_link takes an additional `Options' section which can be
%% used to set certain publisher-specific options:
%% <dl>
%%   <dt>confirms</dt><dd>should be enable publisher confirms?</dd>
%% </dl>
%% @end
start_link(Name, Connection, Declarations, InOptions) ->
    Options = maps:merge(?DEFAULT_OPTIONS, InOptions),
    lager:info("Options: ~p", [Options]),
    gen_server:start_link(?MODULE,
    	[Name, Connection, Options#{ declarations := Declarations }], []).

%% @doc publish a message asynchronously to RabbitMQ
%% The specification is that you have to provide all parameters, because experience
%% has shown that you end up having to tweak these things quite a lot in practice.
%% Hence we provide the full kind of messaging, rather than a subset
%% @end
publish(Publisher, Exch, Key, ContentType, Payload, Opts) ->
    Pub = mk_publish(Exch, Key, ContentType, Payload, Opts),
    Pid = gproc:where({n,l,{turtle,publisher,Publisher}}),
    gen_server:cast(Pid, Pub).

publish_sync(Publisher, Exch, Key, ContentType, Payload, Opts) ->
    Pub = mk_publish(Exch, Key, ContentType, Payload, Opts),
    Pid = gproc:where({n,l,{turtle,publisher,Publisher}}),
    gen_server:call(Pid, Pub).
    
%% CALLBACKS
%% -------------------------------------------------------------------

%% @private
init([Name, ConnName, Options]) ->
    %% Initialize the system in the {initializing,...} state and await the presence of
    %% a connection under the given name without blocking the process. We replace
    %% the state with a #state{} record once that happens (see handle_info/2)
    Ref = gproc:nb_wait({n,l,{turtle,connection,ConnName}}),
    ok = exometer:ensure([ConnName, Name, casts], spiral, []),
    {ok, {initializing, Name, Ref, ConnName, Options}}.

%% @private
handle_call(_Pub, _From, {initializing, _, _, _, _} = Init) ->
    {reply, {error, initializing}, Init};
handle_call({publish, Pub, Props, Payload}, From,
	#state {
	    channel = Ch,
	    confirms = Confirm,
	    conn_name = ConnName,
	    name = Name } = InState) ->

    {ok, State} = 
        publish({call, From}, Ch, Pub, #amqp_msg { props = Props, payload = Payload },
            InState),
    exometer:update([ConnName, Name, calls], 1),

    case Confirm of
        true -> {noreply, State};
        false -> {reply, ok, State}
    end;
handle_call(Call, From, State) ->
    lager:warning("Unknown call from ~p: ~p", [From, Call]),
    {reply, {error, unknown_call}, State}.

%% @private
handle_cast(Pub, {initializing, _, _, _, _} = Init) ->
    %% Messages cast to an initializing publisher are thrown away, but it shouldn't
    %% happen, so we log them
    lager:warning("Publish while initializing: ~p", [Pub]),
    {noreply, Init};
handle_cast({publish, Pub, Props, Payload},
	#state { channel = Ch, conn_name = ConnName, name = Name } = InState) ->
    {ok, State} = publish({cast, undefined}, Ch, Pub, #amqp_msg { props = Props, payload = Payload },
        InState),
    exometer:update([ConnName, Name, casts], 1),
    {noreply, State};
handle_cast(Cast, State) ->
    lager:warning("Unknown cast: ~p", [Cast]),
    {noreply, State}.

%% @private
handle_info(#'basic.ack' { delivery_tag = Seq, multiple = Multiple},
	#state { confirms = true } = InState) ->
    {ok, State} = confirm(ack, Seq, Multiple, InState),
    {noreply, State};
handle_info(#'basic.nack' { delivery_tag = Seq, multiple = Multiple },
	#state { confirms = true } = State) ->
    {ok, State} = confirm(nack, Seq, Multiple, State),
    {noreply, State};
handle_info({gproc, Ref, registered, {_, Pid, _}}, {initializing, N, Ref, CName, Options}) ->
    {ok, Channel} = turtle:open_channel(CName),
    #{ declarations := Decls, confirms := Confirms} = Options,
    ok = turtle:declare(Channel, Decls),
    case Confirms of
        true ->
            #'confirm.select_ok' {} =
                amqp_channel:call(Channel, #'confirm.select'{}),
                ok = amqp_channel:register_confirm_handler(Channel, self());
        false ->
            ok
    end,
    MRef = erlang:monitor(process, Pid),
    reg(N),
    {noreply,
      #state {
        channel = Channel,
        conn_ref = MRef,
        conn_name = CName,
        confirms = Confirms,
        name = N}};
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

%% Create a new publish package
mk_publish(Exch, Key, ContentType, Payload, Opts) ->
    Pub = #'basic.publish' {
        exchange = Exch,
        routing_key = Key
    },
    Props = properties(ContentType, Opts),
    {publish, Pub, Props, Payload}.

%% Perform the AMQP publication and track confirms
publish({cast, undefined}, Ch, Pub, Props, #state{ confirms = true } = State) ->
   _Seq = amqp_channel:next_publish_seqno(Ch),
   ok = amqp_channel:cast(Ch, Pub, Props),
    {ok, State};
publish({call, From}, Ch, Pub, Props, #state{ confirms = true, unacked = UA } = State) ->
   Seq = amqp_channel:next_publish_seqno(Ch),
   ok = amqp_channel:call(Ch, Pub, Props),
   T = turtle_time:monotonic_time(),
   {ok, State#state{ unacked = gb_trees:insert(Seq, {From, T}, UA) }};
publish({F, _X}, Ch, Pub, Props, #state{ confirms = false } = State) ->
   ok = amqp_channel:F(Ch, Pub, Props),
   {ok, State}.
    
confirm(Reply, Seq, Multiple, #state { unacked = UA } = State) ->
    T2 = turtle_time:monotonic_time(),
    {Results, UA1} = remove_delivery_tags(Seq, Multiple, UA),
    [rep(Reply, From, T1, T2) || {From, T1} <- Results],
    {ok, State#state { unacked = UA1 }}.

remove_delivery_tags(Seq, false, Unacked) ->
    case gb_trees:lookup(Seq, Unacked) of
        {value, X} -> {[X], gb_trees:delete(Seq, Unacked)};
        none ->
            %% This can happen if for instance we have casted a message. That
            %% Particular SeqNo is not present among the unacked messages
            {[], Unacked}
    end;
remove_delivery_tags(Seq, true, Unacked) ->
    case gb_trees:is_empty(Unacked) of
        true -> {[], Unacked};
        false ->
            {Smallest, X, UA1} = gb_trees:take_smallest(Unacked),
            case Smallest > Seq of
                true ->
                    {[], Unacked};
                false ->
                    {Xs, FinalUA} = remove_delivery_tags(Seq, true, UA1),
                    {[X | Xs], FinalUA}
            end
    end.

rep(Reply, From, T1, T2) ->
    Window = turtle_time:convert_time_unit(T2 - T1, native, milli_seconds),
    gen_server:reply(From, {Reply, Window}).
