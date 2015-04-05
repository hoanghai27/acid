%% @author Tran Hoan <tvhoan88@gmail.com>
%% Created on 5:54:04 PM Feb 17, 2015
%% Purpose

%% maintainer


-module(lager_acid_backend).
-behaviour(gen_event).

-include_lib("lager/include/lager.hrl").

-export([init/1,
		 handle_event/2,
		 handle_call/2,
		 handle_info/2,
		 terminate/2,
		 code_change/3
]).

-export([new_handler/1,
		 config_to_id/1
]).

-export([test_init/0,
		 test_log/1
]).

-define(ACID_DEFAULT_FORMAT,[date," ",time,"#", severity, "#",node,":",{pid, ""},
							 {module, [ {pid, ["#"], ""}, module,
										{function, [":", function], ""},
										{line, [":",line], ""}], ""},
							 "#", message]).

-record(acid_be_st,{
	name		:: string(),	%% Lager acid backend name
	level		:: integer(),	%% Lager loglevel
	formatter	:: atom(),		%% Lager formater
	formatter_config	:: list(),	%% Lager format config
	sock_pid	:: pid()
}).		

new_handler(Config) ->
	supervisor:start_child(lager_handler_watcher_sup, [lager_event,config_to_id(Config),Config]).

config_to_id(Config) ->
    case proplists:get_value(name, Config) of
        undefined ->
            {?MODULE,"acid"};
        Name ->
            {?MODULE, Name}
    end.

%% init/1
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_event.html#Module:init-1">gen_event:init/1</a>
-spec init(InitArgs) -> Result when
	InitArgs :: Args | {Args, Term :: term()},
	Args :: term(),
	Result :: {ok, State}
			| {ok, State, hibernate}
			| {error, Reason :: term()},
	State :: term().
%% ====================================================================
init(Configs) ->
	[Name0,Level0,Formatter0,FormatConfig0] = [proplists:get_value(X,Configs) || X <- [name,level,formatter,format_config]],
	Name = if
			   is_list(Name0) -> Name0;
			   true -> "acid"
		   end,
	Level = case validate_loglevel(Level0) of
				false -> {mask,255};
				L1 -> L1
			end,
	Formatter = if
				   Formatter0 == undefined ->
					   lager_default_formatter;
				   true ->
					   Formatter0
			   end,
	FormatConfig = if
					   is_list(FormatConfig0) -> FormatConfig0;
					   true -> ?ACID_DEFAULT_FORMAT
					end,
	case lager_acid_socket:start_link(Configs) of
		{ok,Pid} ->
			link(Pid),
			{ok, #acid_be_st{name = Name,level = Level,formatter = Formatter,formatter_config = FormatConfig,sock_pid = Pid}};
		_ ->
			{error,bad_config}
	end.


%% handle_event/2
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_event.html#Module:handle_event-2">gen_event:handle_event/2</a>
-spec handle_event(Event :: term(), State :: term()) -> Result when
	Result :: {ok, NewState}
			| {ok, NewState, hibernate}
			| {swap_handlers, Args1, NewState, Handler2, Args2}
			| remove_handler,
	NewState :: term(), Args1 :: term(), Args2 :: term(),
	Handler2 :: Module2 | {Module2, Id :: term()},
	Module2 :: atom().
%% ====================================================================
handle_event({log,Msg},#acid_be_st{name=Name, level=L,formatter=Formatter,formatter_config=FormatConfig} = State) ->
	case lager_util:is_loggable(Msg,L,{lager_acid_backend, Name}) of
        true ->
			write(State, Formatter:format(Msg,FormatConfig)),
            {ok, State};
        false ->
            {ok, State}
    end;

handle_event(Event, State) ->
	?INT_LOG(warning,"drop unexpected event ~p",[Event]),
    {ok, State}.


%% handle_call/2
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_event.html#Module:handle_call-2">gen_event:handle_call/2</a>
-spec handle_call(Request :: term(), State :: term()) -> Result when
	Result :: {ok, Reply, NewState}
			| {ok, Reply, NewState, hibernate}
			| {swap_handler, Reply, Args1, NewState, Handler2, Args2}
			| {remove_handler, Reply},
	Reply :: term(),
	NewState :: term(), Args1 :: term(), Args2 :: term(),
	Handler2 :: Module2 | {Module2, Id :: term()},
	Module2 :: atom().
%% ====================================================================
%% @private
handle_call({set_loglevel, Level}, #acid_be_st{name=Ident} = State) ->
    case validate_loglevel(Level) of
        false ->
            {ok, {error, bad_loglevel}, State};
        Levels ->
            ?INT_LOG(notice, "Changed loglevel of ~s to ~p", [Ident, Level]),
            {ok, ok, State#acid_be_st{level=Levels}}
    end;
handle_call(get_loglevel, #acid_be_st{level=Level} = State) ->
    {ok, Level, State};
handle_call(_Request, State) ->
    {ok, ok, State}.


%% handle_info/2
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_event.html#Module:handle_info-2">gen_event:handle_info/2</a>
-spec handle_info(Info :: term(), State :: term()) -> Result when
	Result :: {ok, NewState}
			| {ok, NewState, hibernate}
			| {swap_handler, Args1, NewState, Handler2, Args2}
			| remove_handler,
	NewState :: term(), Args1 :: term(), Args2 :: term(),
	Handler2 :: Module2 | {Module2, Id :: term()},
	Module2 :: atom().
%% ====================================================================
handle_info(Info, State) ->
	?INT_LOG(warning,"drop unexpected event ~p",[Info]),
    {ok, State}.


%% terminate/2
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_event.html#Module:terminate-2">gen_event:terminate/2</a>
-spec terminate(Arg, State :: term()) -> term() when
	Arg :: Args
		| {stop, Reason}
		| stop
		| remove_handler
		| {error, {'EXIT', Reason}}
		| {error, Term :: term()},
	Args :: term(), Reason :: term().
%% ====================================================================
terminate(_Arg, #acid_be_st{sock_pid = Pid}) ->
	lager_acid_socket:shutdown(Pid),
    ok.


%% code_change/3
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_event.html#Module:code_change-3">gen_event:code_change/3</a>
-spec code_change(OldVsn, State :: term(), Extra :: term()) -> {ok, NewState :: term()} when
	OldVsn :: Vsn | {down, Vsn},
	Vsn :: term().
%% ====================================================================
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% ====================================================================
%% Internal functions
%% ====================================================================
validate_loglevel(Level) ->
    try lager_util:config_to_mask(Level) of
        Levels ->
            Levels
    catch
        _:_ ->
            false
    end.

write(#acid_be_st{sock_pid = Pid},Msg) ->
	MB = unicode:characters_to_binary(Msg),
	lager_acid_socket:write(Pid,MB).

%% Test
test_init() ->
	application:set_env(lager,handlers,[{lager_console_backend, debug}]),
	application:set_env(lager,crash_log,"log/crash.log"),
	{ok,_} = application:ensure_all_started(lager),
	new_handler([{name,"tvhoan88"},{level,debug},{b_time,1000},{b_size,1024},{p_ip,{127,0,0,1}},{p_port,8001},{s_ip,{127,0,0,1}},{s_port,8001}]),
%% 	gen_event:add_handler(lager_event,lager_console_backend,[debug, {lager_default_formatter,[date," ",time," [",severity, "] [", node,":",pid,"] [",module,":",function,":",line,"] ",message,"\n"]}]),
	ok.

test_log(Msg) ->
%% 	lager:debug("print ~p",[Msg]),
%% 	lager:info("print ~p",[Msg]),
%% 	lager:warning("print ~p",[Msg]),
%% 	lager:notice("print ~p",[Msg]),
	lager:error("print ~p",[Msg]),
	ok.
