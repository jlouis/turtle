%%% @doc Tasks that can be executed by a service
%% This module implements the Celery task model and aims to be compatible
%% with it. It allows Erlang nodes to publish tasks into Celery, as well as decoding
%% them.
%%% @end
-module(turtle_task).

-export([task/2, task/3, task/4]).
-export([
	pack/1,
	unpack/2
]).

-type task() :: #{ atom() => any() }.

-spec pack(task()) -> {binary(), binary()}.
pack(Task) ->
    {<<"application/x-msgpack">>, msgpack:pack(Task, [{format, map}])}.

-spec unpack(CType, Payload) -> {ok, task()} | {error, Reason}
  when
    CType :: binary(),
    Payload :: binary(),
    Reason :: term().
unpack(<<"application/x-msgpack">>, Task) ->
    msgpack:unpack(Task, [{format, map}]).

%% @equiv task(Name, Args, #{}, #{})
task(Name, Args) -> task(Name, Args, #{}, #{}).

%% @equiv task(Name, Args, #{}, Options)
task(Name, Args, Options) -> task(Name, Args, #{}, Options).

%% @doc task/4 produces a task term for Celery
%% Calling `task(Name, Args, KWArgs, Options)' produces a new task term which can
%% be packed and sent over the wire on AMQP. The model here closely follows the
%% format of Celery.
%%
%% The `Options' is a map where every key is optional. Currently the following options
%% are supported
%%
%% <dl>
%% <dt>timelimit := {Soft, Hard}</dt>
%%   <dd>Set two time limits as floats. These will be used when processing tasks under time limits.</dd>
%% </dl>
%% @end
-spec task(Name, Args, KWArgs, Options) -> task()
  when
    Name :: binary(),
    Args :: [term()],
    KWArgs :: #{ atom() => term() },
    Options :: #{ atom() => term() }.

task(Name, Args, KWArgs, Options) ->
    UUID = uuid:get_v4(weak),
    maps:merge(
        #{
          task => Name,
          id => UUID,
          
          args => Args,
          kwargs => KWArgs,
          utc => true
        },
        task_options(Options)
    ).

task_options(OptMap) ->
    ok = valid_options(maps:to_list(OptMap)),
    OptMap.

valid_options([{eta, _} | _]) -> error({?MODULE, no_eta_support});
valid_options([{expires, _} | _]) -> error({?MODULE, no_expires_support});
valid_options([{timelimit, {S, H}} | Os]) when is_float(S), is_float(H) ->
    valid_options(Os);
valid_options([]) -> ok.

