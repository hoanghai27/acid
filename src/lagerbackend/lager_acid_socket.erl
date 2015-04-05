%% @author Tran Hoan <tvhoan88@gmail.com>
%% Created on 7:56:44 PM Feb 17, 2015
%% Purpose

%% maintainer

-module(lager_acid_socket).
-behaviour(gen_server).
-include_lib("lager/include/lager.hrl").
-export([init/1,
		 handle_call/3,
		 handle_cast/2,
		 handle_info/2,
		 terminate/2,
		 code_change/3
]).
-export([start_link/1,
		 shutdown/1,
		 write/2]).

-define(RECONNECT_CHECK,5000).
-define(TCP_CONNECT_TIME,500).
-define(BIN_DELIM,<<0,0>>).

-record(acid_socket_st,{
	p_ip	:: inet:ip4_address(),
	p_port	:: inet:port_number(),
	s_ip	:: inet:ip4_address(),
	s_port	:: inet:port_number(),
	p_fd	:: port(),		%% Primary TCP socket descriptor
	s_fd	:: port(),		%% Secondary TCP socket descriptor
	b_time		:: timeout(),	%% Socket Time Rolling
	b_size		:: integer(),	%% Socket Buffer Size
	buf	= <<>>	:: iolist(),	%% Socket Buffer
	refFlush	:: timeout()	%% Socket flushing timeout reference
}).

write(Pid,Msg) ->
	Pid ! {append,Msg}.

shutdown(Pid) ->
	gen_server:call(Pid,shutdown).

start_link(Configs) ->
	gen_server:start_link(?MODULE,Configs,[]).

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
init(Configs) ->
	[Bsize0,Btime0,Pip,Pport,Sip,Sport] = [proplists:get_value(X,Configs) || X <- [b_size,b_time,p_ip,p_port,s_ip,s_port]],
	try
		Btime = if
			is_integer(Btime0), Btime0 > 0, Btime0 < 300000 ->
				Btime0;
			true ->
				10000
		end,
		Bsize = if
					is_integer(Bsize0), Bsize0 > 0, Bsize0 < 1024*1024 ->
						Bsize0;
					true ->
						1024
				end,
		Pip1 = if is_list(Pip) -> 
					  {ok,I1} = inet:parse_ipv4_address(Pip),
					  I1;
				  is_tuple(Pip) ->
					  true = is_list(inet:ntoa(Pip)),
					  Pip
				end,
		Sip1 = if is_list(Sip) -> 
					  {ok,I2} = inet:parse_ipv4_address(Sip),
					  I2;
				  is_tuple(Sip) ->
					  true = is_list(inet:ntoa(Sip)),
					  Sip
				end,
		true = (is_integer(Pport) andalso Pport > 0 andalso Pport < 65535),
		true = (is_integer(Sport) andalso Sport > 0 andalso Sport < 65535),
		self()  ! acid_reconnect,
		Ref = set_timer(check_acid_flush,Btime),
    	{ok, #acid_socket_st{p_ip = Pip1,p_port = Pport,s_ip = Sip1,s_port = Sport,b_size = Bsize,b_time = Btime,refFlush = Ref}}
	catch
		_:_ ->
			{stop,bad_config}
	end.


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
handle_call(shutdown,_From,State) ->
	{stop,normal,ok,State};

handle_call(_Msg, _From, State) ->
    {reply, {error,bad_msg}, State}.


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
handle_cast(_Msg, State) ->
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
handle_info(acid_reconnect,#acid_socket_st{p_ip = Pip,p_port = Pport,s_ip = Sip,
										   s_port = Sport,p_fd = Pfd,s_fd = Sfd} = State) ->
	Pfd1 = if
			   Pfd == undefined ->
				   case gen_tcp:connect(Pip,Pport,[{packet,tpkt},{active,once},{keepalive,true}],?TCP_CONNECT_TIME) of
					   {ok,S1} ->
						   %% send first packet for nodename
						   gen_tcp:send(S1,pack_tpkt(iolist_to_binary(atom_to_list(node())))),
						   S1;
					   {error,_R1} ->
						   undefined
				   end;
			   true ->
				   Pfd
		   end,
	Sfd1 = if
			   Sfd == undefined ->
				   case gen_tcp:connect(Sip,Sport,[{packet,tpkt},{active,once},{keepalive,true}],?TCP_CONNECT_TIME) of
					   {ok,S2} ->
						   %% send first packet for nodename
						   gen_tcp:send(S2,pack_tpkt(iolist_to_binary(atom_to_list(node())))),
						   S2;
					   {error,_R2} ->
						   undefined
				   end;
			   true ->
				   Sfd
		   end,
	if
		is_port(Pfd1) andalso is_port(Sfd1) ->
			ok;
		true ->
			set_timer(acid_reconnect,?RECONNECT_CHECK)
	end,
	{noreply, State#acid_socket_st{p_fd = Pfd1,s_fd = Sfd1}};

handle_info({append,Msg},#acid_socket_st{b_size = Bsize,b_time = Btime,refFlush = Ref,buf = Buf} = State) ->
	NBsize = size(Buf) + size(Msg),
	NBuf = if
			   size(Buf) == 0 ->
				   Msg;
			   true ->
				   <<Buf/binary,?BIN_DELIM/binary,Msg/binary>>
		   end,
	if
		NBsize >= Bsize ->
			clear_timer(Ref),
			Ref1 = set_timer(check_acid_flush, Btime),
			handle_info({acid_flush,NBuf},State#acid_socket_st{refFlush = Ref1,buf = <<>>});
		true ->
			{noreply,State#acid_socket_st{buf = NBuf}}
	end;

handle_info({acid_flush,_Msg},#acid_socket_st{p_fd = undefined,s_fd = undefined} = State) ->
	%% drop message when has no connection
%% 	io:format("no connection => drop message ~p ~n",[_Msg]),
	{noreply,State};

handle_info({acid_flush,Msg},#acid_socket_st{p_fd = Pfd,s_fd = undefined} = State) ->
	case gen_tcp:send(Pfd,pack_tpkt(Msg)) of
		ok -> 
			{noreply,State};
		{error,_R1} ->
%% 			io:format("write to primary socket failed => ~p~n",[R1]),
			{noreply,State#acid_socket_st{p_fd = undefined}}
	end;

handle_info({acid_flush,Msg},#acid_socket_st{p_fd = undefined,s_fd = Sfd} = State) ->
	case gen_tcp:send(Sfd,pack_tpkt(Msg)) of
		ok -> 
			{noreply,State};
		{error,_R1} ->
%% 			io:format("write to secondary socket failed => ~p~n",[R1]),
			{noreply,State#acid_socket_st{s_fd = undefined}}
	end;

handle_info({acid_flush,Msg},#acid_socket_st{p_fd = Pfd,s_fd = Sfd} = State) ->
	case gen_tcp:send(Pfd,pack_tpkt(Msg)) of
		ok -> 
			{noreply,State};
		{error,_R1} ->
			set_timer(acid_reconnect,?RECONNECT_CHECK),
%% 			io:format("write to primary socket failed => ~p~n",[R1]),
			case gen_tcp:send(Sfd,pack_tpkt(Msg)) of
				ok -> 
					{noreply,State#acid_socket_st{p_fd = undefined}};
				{error,_R2} ->
%% 					io:format("write to secondary socket failed => ~p~n",[R2]),
					{noreply,State#acid_socket_st{p_fd = undefined,s_fd = undefined}}
			end
	end;

handle_info(check_acid_flush,#acid_socket_st{buf = Msg,b_time = Btime} = State) when is_binary(Msg),size(Msg) > 0 ->
	Ref1 = set_timer(check_acid_flush, Btime),
	handle_info({acid_flush,Msg},State#acid_socket_st{refFlush = Ref1,buf = <<>>});

handle_info(check_acid_flush,#acid_socket_st{b_time = Btime} = State) ->
	Ref1 = set_timer(check_acid_flush, Btime),
	{noreply, State#acid_socket_st{refFlush = Ref1}};

%% ==========================================================================
%% Handle TCP message
%% ==========================================================================
handle_info({tcp, Socket, _Data},State) ->
	%% process data here
%% 	io:format("handle_info: receive TCP data ~p",[{tcp, Socket, _Data}]),
	inet:setopts(Socket,[{active,once}]),
	{noreply,State};

handle_info({tcp_passive,Socket},State) ->
	inet:setopts(Socket,[{active,once}]),
	{noreply,State};

handle_info({tcp_closed,Sfd},#acid_socket_st{s_fd = Sfd,p_fd = Pfd} = State) ->
	if
		Pfd == undefined ->
			ok;
		true ->
			set_timer(acid_reconnect,?RECONNECT_CHECK)
	end,
	{noreply,State#acid_socket_st{s_fd = undefined}};

handle_info({tcp_closed,Pfd},#acid_socket_st{s_fd = Sfd,p_fd = Pfd} = State) ->
	if
		Sfd == undefined ->
			ok;
		true ->
			set_timer(acid_reconnect,?RECONNECT_CHECK)
	end,
	{noreply,State#acid_socket_st{p_fd = undefined}};

handle_info({tcp_error,Sfd,_Reason},#acid_socket_st{s_fd = Sfd,p_fd = Pfd} = State) ->
	if
		Pfd == undefined ->
			ok;
		true ->
			set_timer(acid_reconnect,?RECONNECT_CHECK)
	end,
	{noreply,State#acid_socket_st{s_fd = undefined}};

handle_info({tcp_error,Pfd,_Reason},#acid_socket_st{s_fd = Sfd,p_fd = Pfd} = State) ->
	if
		Sfd == undefined ->
			ok;
		true ->
			set_timer(acid_reconnect,?RECONNECT_CHECK)
	end,
	{noreply,State#acid_socket_st{p_fd = undefined}};

handle_info(_Msg, State) ->
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
terminate(_Reason, #acid_socket_st{p_fd = Pfd,s_fd = Sfd}) ->
%% 	io:format("terminated by ~p~n",[Reason]),
	if is_port(Pfd) -> gen_tcp:close(Pfd);
	   true -> ok
	end,
	if is_port(Sfd) -> gen_tcp:close(Sfd);
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


clear_timer(Ref) when is_reference(Ref) -> erlang:cancel_timer(Ref);
clear_timer(_) -> ok.
set_timer(Msg,TO) -> erlang:send_after(TO,self(),Msg).

pack_tpkt(Msg) ->
	Len = byte_size(Msg) + 4,
	<<3,0,Len:16,Msg/binary>>.