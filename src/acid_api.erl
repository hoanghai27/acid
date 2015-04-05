%% @author Tran Hoan <tvhoan88@gmail.com>
%% Created on 9:00:06 AM Feb 21, 2015
%% Purpose

%% maintainer

-module(acid_api).

-include("acid.hrl").

-export([process/4]).

-export([init_db/0,
		 set_cause/2,
		 get_cause/2,
		 get_config/1,
		 get_config/2,
		 set_config/2,
		 set_ftpinfo/4,
		 get_ftpinfo/2,
		 del_ftpinfo/2,
		 connect_logger_db/1,
		 dump_offline/1,
		 dump_offline/2
]).

-compile(export_all).

init_db() ->
	ets:new(?ACID_CONFIG_DB, [named_table,public]),
	ets:new(?ACID_CAUSE_DB, [named_table,public]),
	ets:new(?ACID_FTP_SESSION,[named_table,public]),
	ets:new(?ACID_FTP_INFO,[named_table,public,{keypos,#acid_ftp_info.id}]).


set_ftpinfo(HostInfo,Fn,Size,MTime) ->
	ets:insert(?ACID_FTP_INFO,#acid_ftp_info{id = HostInfo ++ ":" ++ Fn,size = Size,mtime = MTime}).

get_ftpinfo(HostInfo,Fn) ->
	case ets:lookup(?ACID_FTP_INFO,HostInfo ++ ":" ++ Fn) of
		[Rec|_] ->
			Rec;
		_ ->
			false
	end.

del_ftpinfo(HostInfo,Fn) ->
	ets:delete(?ACID_FTP_INFO,HostInfo ++ ":" ++ Fn).

get_config(K) ->
	ets:lookup_element(?ACID_CONFIG_DB,K,2).
get_config(K,Default) ->
	case catch get_config(K) of
		{'EXIT',_} ->
			Default;
		V ->
			V
	end.
set_cause(K,V) when is_list(K) ->
	ets:insert(?ACID_CAUSE_DB,{iolist_to_binary(K),V}).
get_cause(K,Default) ->
	case catch ets:lookup_element(?ACID_CAUSE_DB,K,2) of
		{'EXIT',_} ->
			iolist_to_binary(integer_to_list(Default));
		V ->
			iolist_to_binary(integer_to_list(V))
	end.
set_config(acid_mapping_cause,V) -> [set_cause(K1, V1) || {K1,V1} <- V];
set_config(K,V) -> ets:insert(?ACID_CONFIG_DB,{K,V}).

connect_logger_db(mysql) ->
	Host = acid_api:get_config(acid_db_server),
	Port = acid_api:get_config(acid_db_port),
	User = acid_api:get_config(acid_db_username),
	Passwd = acid_api:get_config(acid_db_password),
	Db = acid_api:get_config(acid_db_schema),
	Encoding = acid_api:get_config(acid_db_encoding,latin1),
	case emysql:add_pool(?ACID_LOGGER_POOL,?ACID_LOGGER_POOL_SIZE,User,Passwd,Host,Port,Db,Encoding) of
		{reply,ok,_} ->	ok;
		{reply,{error, pool_already_exists},_} -> ok;
		Exp -> {error,Exp}
	end.

dump_offline(Fn) -> dump_offline(Fn,false).
dump_offline(Fn,Overwrite) ->
	%% create db connection
	connect_logger_db(get_config(acid_db_type,mysql)),
	SQL = <<"SELECT startwrite,endwrite FROM offlinelog">>,
	emysql:execute(?ACID_LOGGER_POOL,SQL).

process(logger,_Db,_Msg,undefined) ->
	%% log startup
	ok;
process(logger,Db,Msg,NodeName) ->
	Lines = binary:split(Msg, ?BIN_DELIM,[global]),
	[procces_log_line(X,Db,NodeName) || X <- Lines],
	ok.

procces_log_line(Line,Db,NodeName) ->
	case binary:split(Line,<<$#>>,[global]) of
		[Date,Level,NodePid,MF,Contents] ->
			insert_db(Db,NodeName,Date,Level,NodePid,MF,Contents);
		[Date,Level,NodePid,MF|Data] ->
			Contents = iolist_to_binary(Data),
			insert_db(Db,NodeName,Date,Level,NodePid,MF,Contents);
		_ ->
			%% invalid log
			insert_db(Db,NodeName,<<"">>,<<"">>,<<"">>,<<"">>,Line)
	end.

-define(RL_SPAWN,<<"1">>).
-define(RL_RSPAWN,<<"2">>).
-define(RL_CHILD,<<"3">>).
-define(RL_HASID,<<"110">>).

insert_db(mysql,NodeName,Date,Level,NodePid,MF,Contents) ->
	Pid = case binary:split(NodePid,<<$:>>) of
			  [_,P] ->
				  P;
			  _ ->
				  <<"">>
		  end,
%% 	{Mod,Fun} = case binary:split(NodePid,<<":">>,[global]) of
%% 					[M,F,_] -> {M,F};
%% 					[M,F] -> {M,F};
%% 					_ -> {<<"NULL">>,<<"NULL">>}
%% 				end,
	SQL = <<"INSERT INTO msslog(logtime,loglevel,node,pid,modname,msg) values('",Date/binary,$',$,,$',
			Level/binary,$',$,,$',NodeName/binary,$',$,,$',Pid/binary,$',$,,$',MF/binary,$',$,,$',Contents/binary,$',$)>>,
	emysql:execute(?ACID_LOGGER_POOL,SQL),
%%  process log contents
	case Contents of
		<<"SPAWN:",_/binary>> ->
			case binary:split(Contents,<<$:>>, [global]) of
				[<<"SPAWN">>,Pid1,Pid2] ->
					MetaSQL = <<"INSERT INTO relationlog(id1,id2,relation,timestamp,node1,node2,remote) values('",
								Pid1/binary,$',$,,$',Pid2/binary,$',$,,?RL_SPAWN/binary,$,,$',Date/binary,"','",NodeName/binary,"','',FALSE)">>,
%% 					lager:debug("MetaSQL = ~p",[MetaSQL]),
					emysql:execute(?ACID_LOGGER_POOL,MetaSQL);
				_ ->
					%% invalid log
					ok
			end;
		<<"RSPAWN:",_/binary>> ->
			case binary:split(Contents,<<$:>>, [global]) of
				[<<"RSPAWN">>,Node1,Pid1,Node2,Pid2] ->
					P1 = normalize_pid(Pid1),
					P2 = normalize_pid(Pid2),
					MetaSQL = <<"INSERT INTO relationlog(id1,id2,relation,timestamp,node1,node2,remote) values('",
								P1/binary,$',$,,$',P2/binary,$',$,,?RL_RSPAWN/binary,$,,$',Date/binary,$',$,,
								Node1/binary,$,,Node2/binary,",TRUE)">>,
%% 					lager:debug("MetaSQL = ~p",[MetaSQL]),
					emysql:execute(?ACID_LOGGER_POOL,MetaSQL);
				_ ->
					%% invalid log
					ok
			end;
		<<"HASID:",_/binary>> ->
			case binary:split(Contents,<<$:>>, [global]) of
				[<<"HASID">>,Pid,RawId] ->
					Id = normalize_id(RawId),
					MetaSQL = <<"INSERT INTO relationlog(id1,id2,relation,timestamp,node1,node2,remote) values('",
								Pid/binary,$',$,,$',Id/binary,$',$,,?RL_HASID/binary,$,,$',Date/binary,"','",NodeName/binary,"','',FALSE)">>,
%% 					lager:debug("MetaSQL = ~p",[MetaSQL]),
					emysql:execute(?ACID_LOGGER_POOL,MetaSQL);
				_ ->
					%% invalid log
					ok
			end;
		<<"TERMINATED:",_/binary>> ->
			case binary:split(Contents,<<$:>>, [global]) of
				[<<"TERMINATED">>,Reason|_] ->
					Cause = normalize_cause(Reason),
					MetaSQL = <<"INSERT INTO releasecause(pid,node,cause,cause_str,timestamp) values('",
								Pid/binary,$',$,,$',NodeName/binary,$',$,,Cause/binary,$,,
								$',Reason/binary,$',$,,$',Date/binary,"')">>,
%% 					lager:debug("MetaSQL = ~p",[MetaSQL]),
					emysql:execute(?ACID_LOGGER_POOL,MetaSQL);
				_ ->
					%% invalid log
					ok
			end;
		<<"FL_START:",_/binary>> ->
			case binary:split(Contents,<<$:>>, [global]) of 
				[<<"FL_START">>,Pid,ID,FL] ->
					MetaSQL = <<"INSERT INTO relationlog(id1,id2,relation,timestamp,node1,node2,remote) values('",
								Pid/binary,$',$,,$',ID/binary,$',$,,FL/binary,$,,$',Date/binary,"','",NodeName/binary,"','',FALSE)">>,
%% 					lager:debug("MetaSQL = ~p",[MetaSQL]),
					emysql:execute(?ACID_LOGGER_POOL,MetaSQL);
				_ ->
					%% invalid log
					ok
			end;
		_ ->
			%% normal log
			ok
	end.

normalize_pid(Pid) ->
	case Pid of
		<<"0.",_/binary>> -> Pid;
		_ ->
			case binary:split(Pid,<<$.>>,[global]) of
				[_,P2,P3] ->
					<<"<0.",P2/binary,$.,P3/binary>>;
				_ ->
					Pid
			end
	end.

normalize_cause(Reason) ->
	case string:to_integer(binary_to_list(Reason)) of
		{error,_} -> get_cause(Reason, 31);
		{I,_} -> binary:encode_unsigned(I)
	end.

normalize_id(RawId) ->
	case binary:split(RawId,<<$,>>,[global]) of
		[Id] -> Id;
		Ids ->
			bcd2str(Ids,<<>>,RawId)
	end.

bcd2str([],In,_Def) -> In;
bcd2str([Dec|Rem],In,Def) ->
	case string:to_integer(binary_to_list(Dec)) of
		{error,_} -> Def;
		{I,_} ->
			A = (I bsr 4) + $0,
			B = (I band 15) + $0,
			bcd2str(Rem,<<In/binary,B,A>>,Def)
	end.

name2type("LOGS_EM" ++ _) ->
	nexgen_em;
name2type("LOGS_FEP" ++ _) ->
	nexgen_fep;
name2type("LOGS_BEP" ++ _) ->
	nexgen_fep;
name2type("mss" ++ _) ->
	mss;
name2type("vlr" ++ _) ->
	vlr;
name2type("rsm" ++ _) ->
	rsm;
name2type(_) ->
	em.

%% example message <<"2015-03-03 15:37:52.038#debug#mss1@10.61.64.31:<0.2170.0>#cs_gsmSSF:terminate:1449#>>

