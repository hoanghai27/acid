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

-compile(export_all).

-record(tcp_sock_st,{
	type			::	logger | tracer,%% TCP socket type for logger or tracer
	sfd				::	port(),			%% Socket File descriptor
	sync_tick = 5000::	timeout(),		%% Sync interval
	file_handler	::	#tcp_file{},	%% TCP file information
	db_handler		::	#tcp_mysql_db{},%% TCP db dump online information
	new = true		::	boolean(),		%% Check for first packet
	node			::	string()		%% Node name
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
	SyncTick = acid_api:get_config(acid_log_sync_tick, 5000),
	erlang:send_after(SyncTick,self(),sync_now),
    {ok, #tcp_sock_st{type = Type,sfd = Fd,file_handler = new_tcp_file(),db_handler = new_tcp_db()}}.


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
	inet:setopts(State#tcp_sock_st.sfd,[{active,once}]),
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
handle_info({tcp, Socket, Data},#tcp_sock_st{type = logger,sfd = Fd,new = true,file_handler = FH} = State) ->
	%% process data here
	inet:setopts(Socket,[{active,once}]),
	try
		NodeName = unpack_tpkt(Data),
		if size(NodeName) =< 255 -> 
			   lager:debug("associate node ~p to ~p",[NodeName,Fd]),
			   {noreply,State#tcp_sock_st{new = false,node = NodeName,
										  file_handler = FH#tcp_file{nodename = binary_to_list(NodeName)}}};
		   true ->
			   lager:error("bad nodename ~p, close socket",[NodeName]),
			   {stop,normal,State}
		end
	catch
		_:_ ->
			lager:error("process message exception ~p",[erlang:get_stacktrace()]),
			{noreply,State}
	end;
%% handle_info({tcp, Socket, Data},#tcp_sock_st{type = Type,sfd = Fd,node = undefined,db = Db} = State) ->
%% 	%% process data here
%% 	inet:setopts(Socket,[{active,once}]),
%% 	try
%% 		Logs = unpack_tpkt(Data),
%% 		NodeName = get_nodename(Logs),
%% 		acid_api:process(Type,Db, Logs,NodeName),
%% 		lager:debug("associate node ~p to ~p",[NodeName,Fd]),
%% 		{noreply,State#tcp_sock_st{node = NodeName}}
%% 	catch
%% 		_:_ ->
%% 			lager:error("process message exception ~p",[erlang:get_stacktrace()]),
%% 			{noreply,State}
%% 	end;
handle_info({tcp, Socket, Data},#tcp_sock_st{node = NodeName,file_handler = FH,db_handler = DBH} = State) ->
	%% process data here
	inet:setopts(Socket,[{active,once}]),
	try
		StripData = unpack_tpkt(Data),
		{ok,TcpFile} = file_handle(FH,StripData,NodeName),
		{ok,TcpDB} = db_handle(DBH,StripData,NodeName),
		{noreply,State#tcp_sock_st{file_handler = TcpFile,db_handler = DBH}}
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
	{stop,normal,State#tcp_sock_st{sfd = undefined}};
handle_info({tcp_error,Socket,_Reason},State) ->
	lager:debug("socket ~p is error, stop processs",[Socket]),
	{noreply,State#tcp_sock_st{sfd = undefined}};
handle_info(sync_now,#tcp_sock_st{sync_tick = SyncTick,file_handler = MyFile,db_handler = MyDB} = State) ->
	erlang:send_after(SyncTick,self(),sync_now),
	{ok,NMyFile} =  file_sync(MyFile),
	{ok,NMyDB} = db_sync(MyDB),
	{noreply,State#tcp_sock_st{file_handler = NMyFile,db_handler = NMyDB}};

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
terminate(_Reason, #tcp_sock_st{sfd = Fd,file_handler = FH}) ->
	file_close(FH),
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

%% get_nodename(Logs) when is_binary(Logs) ->
%% 	Lines = binary:split(Logs, ?BIN_DELIM,[global]),
%% 	get_nodename(Lines);
%% get_nodename([]) ->
%% 	undefined;
%% get_nodename([X|Rem]) ->
%% 	case binary:split(X,<<"#">>,[global]) of
%% 		[_,_,NodeRaw,_,_] ->
%% 			case binary:split(NodeRaw,<<":">>) of
%% 				[<<"Undefined">>,_] ->
%% 					get_nodename(Rem);
%% 				[NodeName,_] ->
%% 					NodeName;
%% 				_ ->
%% 					get_nodename(Rem)
%% 			end;
%% 		_ ->
%% 			get_nodename(Rem)
%% 	end.
file_close(MyFile) ->
	if MyFile#tcp_file.fd == undefined -> ok;
	   true ->
		   _ = file:close(MyFile#tcp_file.fd),
		   _ = file:close(MyFile#tcp_file.fd)
	end.

file_handle(MyFile,Msg,_) ->
	Lines = binary:split(Msg, ?BIN_DELIM,[global]),
	file_write_line(MyFile,Lines).

file_write_line(MyFile,[]) -> {ok,MyFile};
file_write_line(MyFile,[L|R]) ->
	case acid_api:write_logfile(MyFile, L) of
		{ok,MyFile1} -> file_write_line(MyFile1,R);
		_ -> file_write_line(MyFile,R)
	end.

db_handle(TcpDB,Msg,NodeName) when is_record(TcpDB,tcp_mysql_db) ->
	Lines = binary:split(Msg, ?BIN_DELIM,[global]),
	db_write_line(TcpDB, Lines, NodeName);
db_handle(TcpDB,_,_) -> {ok,TcpDB}.

db_write_line(TcpDB,[],_) -> {ok,TcpDB};
db_write_line(TcpDB,[L|R],NodeName) ->
	case acid_api:write_db(TcpDB, L, NodeName) of
		{ok,TcpDB1} -> db_write_line(TcpDB1,R,NodeName);
		_ -> db_write_line(TcpDB,R,NodeName)
	end.

%% not need for sync, let os sync
file_sync(MyFile) ->
	{ok,MyFile}.
db_sync(#tcp_mysql_db{logbuf = LogB,relationbuf = RelationB,causebuf = CauseB} = MyDB) ->
	if	size(LogB) > 0 ->
			lager:debug("~p",[<<"INSERT INTO msslog(logtime,loglevel,node,pid,modname,msg) VALUES ",LogB/binary>>]),
			emysql:execute(?ACID_LOGGER_POOL,<<"INSERT INTO msslog(logtime,loglevel,node,pid,modname,msg) VALUES ",LogB/binary>>);
		true -> ok
	end,
	if size(RelationB) > 0 ->
		   lager:debug("~p",[<<"INSERT INTO relationlog(id1,id2,relation,timestamp,node1,node2,remote) VALUES ",RelationB/binary>>]),
		   emysql:execute(?ACID_LOGGER_POOL,<<"INSERT INTO relationlog(id1,id2,relation,timestamp,node1,node2,remote) VALUES ",RelationB/binary>>);
	   true -> ok
	end,
	if size(CauseB) > 0 ->
		   lager:debug("~p",[<<"INSERT INTO releasecause(pid,node,cause,cause_str,timestamp) VALUES ",CauseB/binary>>]),
		   emysql:execute(?ACID_LOGGER_POOL,<<"INSERT INTO releasecause(pid,node,cause,cause_str,timestamp) VALUES ",CauseB/binary>>);
	   true -> ok
	end,
	{ok,MyDB#tcp_mysql_db{logbuf = <<>>,bulk_log = 0,relationbuf = <<>>,bulk_relation = 0,causebuf = <<>>,bulk_cause = 0}}.

new_tcp_file() ->
	#tcp_file{
		inode = 0,
		date = acid_api:get_config(acid_log_roll_date),
		rsize = acid_api:get_config(acid_log_roll_size),
		sync_size = acid_api:get_config(acid_log_sync_size, 65536),
		sync_interval = acid_api:get_config(acid_log_sync_tick)
	}.
new_tcp_db() ->
	case acid_api:get_config(acid_log_to_db, false) of
		true ->
			#tcp_mysql_db{
				bulk_size = acid_api:get_config(acid_db_bulk, 10)
			};
		_ ->
			false
	end.