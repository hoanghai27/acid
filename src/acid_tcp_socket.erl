%% @author Tran Hoan <tvhoan88@gmail.com>
%% Created on 5:48:19 PM Feb 17, 2015
%% Purpose

%% maintainer

-module(acid_tcp_socket).
-behaviour(gen_server).

-include("acid.hrl").

-export([init/1,
		 handle_call/3,
		 handle_cast/2,
		 handle_info/2,
		 terminate/2,
		 code_change/3
]).

-export([start_link/1]).

-record(tcp_sock_st,{
	type		::	logger | tracer,%% TCP socket type for logger or tracer
	fd			::	port(),			%% File descriptor
	db			::	mysql | riak,	%% DB tyoe
	new = true	::	boolean(),		%% Check for first packet
	node		::	string()		%% Node name
}).

start_link(Args) ->
	gen_server:start_link(?MODULE,Args,[]).

%% init/1
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:init-1">gen_server:init/1</a>
-spec init(Args :: term()) -> Result when
	Result :: {ok, State}
			| {ok, State, Timeout}
			| {ok, State, hibernate}
			| {stop, Reason :: term()}
			| ignore,
	State :: term(),
	Timeout :: non_neg_integer() | infinity.
%% ====================================================================
init({Type,Fd,_Opts}) ->
    {ok, #tcp_sock_st{type = Type,fd = Fd,db = acid_api:get_config(acid_db_type, mysql)}}.


%% handle_call/3
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:handle_call-3">gen_server:handle_call/3</a>
-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()}, State :: term()) -> Result when
	Result :: {reply, Reply, NewState}
			| {reply, Reply, NewState, Timeout}
			| {reply, Reply, NewState, hibernate}
			| {noreply, NewState}
			| {noreply, NewState, Timeout}
			| {noreply, NewState, hibernate}
			| {stop, Reason, Reply, NewState}
			| {stop, Reason, NewState},
	Reply :: term(),
	NewState :: term(),
	Timeout :: non_neg_integer() | infinity,
	Reason :: term().
%% ====================================================================
handle_call(Request, _From, State) ->
	lager:warning("drop unknown message ~p",[Request]),
    {reply, {error,bad_request}, State}.


%% handle_cast/2
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:handle_cast-2">gen_server:handle_cast/2</a>
-spec handle_cast(Request :: term(), State :: term()) -> Result when
	Result :: {noreply, NewState}
			| {noreply, NewState, Timeout}
			| {noreply, NewState, hibernate}
			| {stop, Reason :: term(), NewState},
	NewState :: term(),
	Timeout :: non_neg_integer() | infinity.
%% ====================================================================
handle_cast(active_socket,State) ->
	lager:debug("active socket for receiving message"),
	inet:setopts(State#tcp_sock_st.fd,[{active,once}]),
	{noreply, State};

handle_cast(Msg, State) ->
	lager:warning("drop unknown message ~p",[Msg]),
    {noreply, State}.


%% handle_info/2
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:handle_info-2">gen_server:handle_info/2</a>
-spec handle_info(Info :: timeout | term(), State :: term()) -> Result when
	Result :: {noreply, NewState}
			| {noreply, NewState, Timeout}
			| {noreply, NewState, hibernate}
			| {stop, Reason :: term(), NewState},
	NewState :: term(),
	Timeout :: non_neg_integer() | infinity.
%% ====================================================================
handle_info({tcp, Socket, Data},#tcp_sock_st{type = logger,fd = Fd,new = true} = State) ->
	%% process data here
	inet:setopts(Socket,[{active,once}]),
	try
		NodeName = unpack_tpkt(Data),
		lager:debug("associate node ~p to ~p",[NodeName,Fd]),
		{noreply,State#tcp_sock_st{new = false,node = NodeName}}
	catch
		_:_ ->
			lager:error("process message exception ~p",[erlang:get_stacktrace()]),
			{noreply,State}
	end;

handle_info({tcp, Socket, Data},#tcp_sock_st{type = Type,fd = Fd,node = undefined,db = Db} = State) ->
	%% process data here
	inet:setopts(Socket,[{active,once}]),
	try
		Logs = unpack_tpkt(Data),
		NodeName = get_nodename(Logs),
		acid_api:process(Type,Db, Logs,NodeName),
		lager:debug("associate node ~p to ~p",[NodeName,Fd]),
		{noreply,State#tcp_sock_st{node = NodeName}}
	catch
		_:_ ->
			lager:error("process message exception ~p",[erlang:get_stacktrace()]),
			{noreply,State}
	end;


handle_info({tcp, Socket, Data},#tcp_sock_st{type = Type,node = NodeName,db = Db} = State) ->
	%% process data here
	inet:setopts(Socket,[{active,once}]),
	try
		acid_api:process(Type,Db, unpack_tpkt(Data),NodeName),
		{noreply,State}
	catch
		_:_ ->
			lager:error("process message exception ~p",[erlang:get_stacktrace()]),
			{noreply,State}
	end;

handle_info({tcp_passive,Socket},State) ->
	inet:setopts(Socket,[{active,once}]),
	{noreply,State};
handle_info({tcp_closed,Socket},State) ->
	lager:debug("socket ~p is closed, stop processs",[Socket]),
	{stop,normal,State#tcp_sock_st{fd = undefined}};
handle_info({tcp_error,Socket,_Reason},State) ->
	lager:debug("socket ~p is error, stop processs",[Socket]),
	{noreply,State#tcp_sock_st{fd = undefined}};
handle_info(Info, State) ->
	lager:warning("drop unknown message ~p",[Info]),
    {noreply, State}.

%% terminate/2
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:terminate-2">gen_server:terminate/2</a>
-spec terminate(Reason, State :: term()) -> Any :: term() when
	Reason :: normal
			| shutdown
			| {shutdown, term()}
			| term().
%% ====================================================================
terminate(_Reason, #tcp_sock_st{fd = Fd}) ->
	if
		is_port(Fd) -> gen_tcp:close(Fd);
		true -> ok
	end,
    ok.


%% code_change/3
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:code_change-3">gen_server:code_change/3</a>
-spec code_change(OldVsn, State :: term(), Extra :: term()) -> Result when
	Result :: {ok, NewState :: term()} | {error, Reason :: term()},
	OldVsn :: Vsn | {down, Vsn},
	Vsn :: term().
%% ====================================================================
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


unpack_tpkt(<<3,0,Len:16,Msg/binary>>) ->
	BinLen = Len - 4,
	if
		BinLen == size(Msg) ->
			Msg;
		true ->
			lager:error("drop bad TPKT packet"),
			<<>>
	end.

get_nodename(Logs) when is_binary(Logs) ->
	Lines = binary:split(Logs, ?BIN_DELIM,[global]),
	get_nodename(Lines);

get_nodename([]) ->
	undefined;

get_nodename([X|Rem]) ->
	case binary:split(X,<<"#">>,[global]) of
		[_,_,NodeRaw,_,_] ->
			case binary:split(NodeRaw,<<":">>) of
				[<<"Undefined">>,_] ->
					get_nodename(Rem);
				[NodeName,_] ->
					NodeName;
				_ ->
					get_nodename(Rem)
			end;
		_ ->
			get_nodename(Rem)
	end.


