%% @author Tran Hoan <tvhoan88@gmail.com>
%% Created on 9:00:06 AM Feb 21, 2015
%% Purpose

%% maintainer

-module(acid_api).

-include("acid.hrl").
-include_lib("kernel/include/file.hrl").

-export([write_db/3,
		 write_logfile/2
		]).

-export([init_db/0,
		 set_cause/2,
		 get_cause/2,
		 get_config/1,
		 get_config/2,
		 set_config/2,
		 set_ftpinfo/4,
		 get_ftpinfo/2,
		 del_ftpinfo/2,
		 connect_logger_db/1
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
%% =============================================================
%%	copy from lager_util
%% =============================================================
get_tcp_fn(NodeName) ->
	[{{Y,M,D},{H,Mi,S}}] = calendar:local_time_to_universal_time_dst(calendar:local_time()),
	DateStr = lists:flatten(io_lib:format("~4.10.0B-~2.10.0B-~2.10.0B",[Y,M,D])),
	TimeStr = lists:flatten(io_lib:format("~2.10.0B.~2.10.0B.~2.10.0B",[H,Mi,S])),
	acid_api:get_config(acid_log_stickdir) ++ "/" ++ NodeName ++ "_" ++ DateStr ++ "_" ++ TimeStr ++ ".log".

rotate_logfile(#tcp_file{fd = FD} = MyFile) -> 
	%% delayed write can cause file:close not to do a close
	_ = file:close(FD),
	_ = file:close(FD),
	open_logfile(MyFile).

write_logfile(#tcp_file{fd = FD,rsize = RotSize} = MyFile,Msg) ->
	Timestamp = os:timestamp(),
	LastCheck = timer:now_diff(Timestamp,MyFile#tcp_file.last_check) div 1000,
	case LastCheck >= MyFile#tcp_file.check_interval orelse FD == undefined of
		true ->
			case ensure_logfile(MyFile) of
				{ok,#tcp_file{size = Size} = MyFile1} when RotSize /= 0, Size > RotSize ->
					case rotate_logfile(MyFile1) of
						{ok,MyFile2} ->
							write_logfile(MyFile2#tcp_file{last_check = Timestamp},Msg);
						_ ->
							lager:error("failed to rotate log file ~p",[MyFile1#tcp_file.fname]),
							{ok,MyFile1#tcp_file{last_check = Timestamp}}
					end;
				{ok,MyFile1} ->
					_ = file:write(FD,Msg),
					_ = file:write(FD,<<"\r\n">>),
					{ok,MyFile1#tcp_file{last_check = Timestamp}};
				{error,Reason} ->
					lager:error("failed to reopen log file ~p",[Reason]),
					{ok,MyFile#tcp_file{last_check = Timestamp}}
			end;
		false ->
			_ = file:write(FD,Msg),
			_ = file:write(FD,<<"\r\n">>),
			{ok,MyFile}
	
	end.

open_logfile(#tcp_file{sync_size = Size,sync_interval = Interval,nodename = NodeName} = MyFile) ->
	Name = get_tcp_fn(NodeName),
	TimeStamp = os:timestamp(),
    case filelib:ensure_dir(Name) of
        ok ->
            Options = [append, raw] ++  [{delayed_write, Size, Interval}],
            case file:open(Name, Options) of
                {ok, FD} ->
                    case file:read_file_info(Name) of
                        {ok, FInfo} ->
                            Inode = FInfo#file_info.inode,
                            {ok, MyFile#tcp_file{fd = FD,fname = Name,inode = Inode,last_check = TimeStamp,size = FInfo#file_info.size}};
                        X -> X
                    end;
                Y -> Y
            end;
        Z -> Z
    end.

ensure_logfile(#tcp_file{fd = FD,inode = Inode,fname = Name} = MyFile) ->
    case file:read_file_info(Name) of
        {ok, FInfo} ->
            Inode2 = FInfo#file_info.inode,
            case Inode == Inode2 of
                true ->
                    {ok,MyFile#tcp_file{size = FInfo#file_info.size}};
                false ->
                    %% delayed write can cause file:close not to do a close
                    _ = file:close(FD),
                    _ = file:close(FD),
                    open_logfile(MyFile)
            end;
        _ ->
            %% delayed write can cause file:close not to do a close
            _ = file:close(FD),
            _ = file:close(FD),
            open_logfile(MyFile)
    end.

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

write_db(MyDB,Line,NodeName) ->
	case binary:split(Line,<<$#>>,[global]) of
		[Date,Level,NodePid,MF,Contents] ->
			insert_db(MyDB,NodeName,Date,Level,NodePid,MF,Contents);
		[Date,Level,NodePid,MF|Data] ->
			Contents = iolist_to_binary(Data),
			insert_db(MyDB,NodeName,Date,Level,NodePid,MF,Contents);
		_ ->
			%% invalid log
			insert_db(MyDB,NodeName,<<"">>,<<"">>,<<"">>,<<"">>,Line)
	end.
-define(RL_SPAWN,<<"1">>).
-define(RL_RSPAWN,<<"2">>).
-define(RL_CHILD,<<"3">>).
-define(RL_HASID,<<"110">>).

insert_db(#tcp_mysql_db{bulk_size = BulkSize} = MyDB,
		  NodeName,Date,Level,NodePid,MF,Contents) ->
	Pid = case binary:split(NodePid,<<$:>>) of
			  [_,P] ->
				  P;
			  _ ->
				 NodePid
		  end,
%% 	{Mod,Fun} = case binary:split(NodePid,<<":">>,[global]) of
%% 					[M,F,_] -> {M,F};
%% 					[M,F] -> {M,F};
%% 					_ -> {<<"NULL">>,<<"NULL">>}
%% 				end,
	if
		size(MyDB#tcp_mysql_db.logbuf) == 0 ->
			LogB = <<"('",Date/binary,$',$,,$',Level/binary,$',$,,$',NodeName/binary,$',
					 $,,$',Pid/binary,$',$,,$',MF/binary,$',$,,$',Contents/binary,$',$)>>,
			LogR = 1;
		true ->
			LogB = <<(MyDB#tcp_mysql_db.logbuf)/binary,",('",Date/binary,$',$,,$',Level/binary,$',$,,$',
					  NodeName/binary,$',$,,$',Pid/binary,$',$,,$',MF/binary,$',$,,$',Contents/binary,$',$)>>,
			LogR = MyDB#tcp_mysql_db.bulk_log + 1
	end,
	if
		LogR >= BulkSize ->
			emysql:execute(?ACID_LOGGER_POOL,<<"INSERT INTO msslog(logtime,loglevel,node,pid,modname,msg) VALUES ",LogB/binary>>),
			NLogB = <<>>,
			NLogR = 0;
		true ->
			NLogB = LogB,
			NLogR = LogR
	end,
%%  process log contents
	case Contents of
		<<"SPAWN:",_/binary>> ->
			case binary:split(Contents,<<$:>>, [global]) of
				[<<"SPAWN">>,Pid1,Pid2] ->
					if
						size(MyDB#tcp_mysql_db.relationbuf) == 0 ->
							RelationB = <<"('",Pid1/binary,$',$,,$',Pid2/binary,$',$,,?RL_SPAWN/binary,$,,$',
										  Date/binary,"','",NodeName/binary,"','',FALSE)">>,
							RelationR = 1;
						true ->
							RelationB = <<(MyDB#tcp_mysql_db.relationbuf)/binary,",('",Pid1/binary,$',$,,$',Pid2/binary,
										  $',$,,?RL_SPAWN/binary,$,,$',Date/binary,"','",NodeName/binary,"','',FALSE)">>,
							RelationR = MyDB#tcp_mysql_db.bulk_relation + 1
					end,
					if
						RelationR > BulkSize ->
							emysql:execute(?ACID_LOGGER_POOL,<<"INSERT INTO relationlog(id1,id2,relation,timestamp,node1,node2,remote) VALUES ",RelationB/binary>>),
							NRelationR = 0,
							NRelationB = <<>>;
						true ->
							NRelationR = RelationR,
							NRelationB = RelationB
					end,
					{ok,MyDB#tcp_mysql_db{logbuf = NLogB,bulk_log = NLogR,relationbuf = NRelationB,bulk_relation = NRelationR}};
				_ ->
					%% invalid log
					{ok,MyDB#tcp_mysql_db{logbuf = NLogB,bulk_log = NLogR}}
			end;
		<<"RSPAWN:",_/binary>> ->
			case binary:split(Contents,<<$:>>, [global]) of
				[<<"RSPAWN">>,Node1,Pid1,Node2,Pid2] ->
					P1 = normalize_pid(Pid1),
					P2 = normalize_pid(Pid2),
					if
						size(MyDB#tcp_mysql_db.relationbuf) == 0 ->
							RelationB = <<"('",P1/binary,$',$,,$',P2/binary,$',$,,?RL_RSPAWN/binary,$,,$',
										  Date/binary,$',$,,Node1/binary,$,,Node2/binary,",TRUE)">>,
							RelationR = 1;
						true ->
							RelationB = <<(MyDB#tcp_mysql_db.relationbuf)/binary,",('",P1/binary,$',$,,$',P2/binary,
										  $',$,,?RL_RSPAWN/binary,$,,$',Date/binary,$',$,,Node1/binary,$,,Node2/binary,",TRUE)">>,
							RelationR = MyDB#tcp_mysql_db.bulk_relation + 1
					end,
					if
						RelationR > BulkSize ->
							emysql:execute(?ACID_LOGGER_POOL,<<"INSERT INTO relationlog(id1,id2,relation,timestamp,node1,node2,remote) VALUES ",RelationB/binary>>),
							NRelationR = 0,
							NRelationB = <<>>;
						true ->
							NRelationR = RelationR,
							NRelationB = RelationB
					end,
					{ok,MyDB#tcp_mysql_db{logbuf = NLogB,bulk_log = NLogR,relationbuf = NRelationB,bulk_relation = NRelationR}};
				_ ->
					%% invalid log
					{ok,MyDB#tcp_mysql_db{logbuf = NLogB,bulk_log = NLogR}}
			end;
		<<"HASID:",_/binary>> ->
			case binary:split(Contents,<<$:>>, [global]) of
				[<<"HASID">>,Pid,RawId] ->
					Id = normalize_id(RawId),
					if
						size(MyDB#tcp_mysql_db.relationbuf) == 0 ->
							RelationB = <<"('",Pid/binary,$',$,,$',Id/binary,$',$,,?RL_HASID/binary,$,,$',Date/binary,
										  "','",NodeName/binary,"','',FALSE)">>,
							RelationR = 1;
						true ->
							RelationB = <<(MyDB#tcp_mysql_db.relationbuf)/binary,",('",Pid/binary,$',$,,$',Id/binary,$',$,,?RL_HASID/binary,$,,$',
										  Date/binary,"','",NodeName/binary,"','',FALSE)">>,
							RelationR = MyDB#tcp_mysql_db.bulk_relation + 1
					end,
					if
						RelationR > BulkSize ->
							emysql:execute(?ACID_LOGGER_POOL,<<"INSERT INTO relationlog(id1,id2,relation,timestamp,node1,node2,remote) VALUES ",RelationB/binary>>),
							NRelationR = 0,
							NRelationB = <<>>;
						true ->
							NRelationR = RelationR,
							NRelationB = RelationB
					end,
					{ok,MyDB#tcp_mysql_db{logbuf = NLogB,bulk_log = NLogR,relationbuf = NRelationB,bulk_relation = NRelationR}};
				_ ->
					%% invalid log
					{ok,MyDB#tcp_mysql_db{logbuf = NLogB,bulk_log = NLogR}}
			end;
		<<"TERMINATED:",_/binary>> ->
			case binary:split(Contents,<<$:>>, [global]) of
				[<<"TERMINATED">>,Reason|_] ->
					Cause = normalize_cause(Reason),
					if
						size(MyDB#tcp_mysql_db.causebuf) == 0 ->
							CauseB = <<"('",Pid/binary,$',$,,$',NodeName/binary,$',$,,Cause/binary,
									   $,,$',Reason/binary,$',$,,$',Date/binary,"')">>,
							CauseR = 1;
						true ->
							CauseB = <<(MyDB#tcp_mysql_db.causebuf)/binary,",('",Pid/binary,$',$,,$',NodeName/binary,
									   $',$,,Cause/binary,$,,$',Reason/binary,$',$,,$',Date/binary,"')">>,
							CauseR = MyDB#tcp_mysql_db.bulk_cause + 1
					end,
					if
						CauseR >= BulkSize ->
							emysql:execute(?ACID_LOGGER_POOL,<<"INSERT INTO releasecause(pid,node,cause,cause_str,timestamp) VALUES ",CauseB/binary>>),
							NCauseB = <<>>,
							NCauseR = 0;
						true ->
							NCauseB = CauseB,
							NCauseR = CauseR
					end,
					{ok,MyDB#tcp_mysql_db{logbuf = NLogB,bulk_log = NLogR,causebuf = NCauseB,bulk_cause = NCauseR}};
				_ ->
					%% invalid log
					{ok,MyDB#tcp_mysql_db{logbuf = NLogB,bulk_log = NLogR}}
			end;
		<<"FL_START:",_/binary>> ->
			case binary:split(Contents,<<$:>>, [global]) of 
				[<<"FL_START">>,Pid,RawId,FL] ->
					ID = normalize_id(RawId),
					if
						size(MyDB#tcp_mysql_db.relationbuf) == 0 ->
							RelationB = <<"('",Pid/binary,$',$,,$',ID/binary,$',$,,FL/binary,$,,$',Date/binary,
										  "','",NodeName/binary,"','',FALSE)">>,
							RelationR = 1;
						true ->
							RelationB = <<(MyDB#tcp_mysql_db.relationbuf)/binary,",('",Pid/binary,$',$,,$',ID/binary,$',$,,FL/binary,$,,$',
										  Date/binary,"','",NodeName/binary,"','',FALSE)">>,
							RelationR = MyDB#tcp_mysql_db.bulk_relation + 1
					end,
					if
						RelationR > BulkSize ->
							emysql:execute(?ACID_LOGGER_POOL,<<"INSERT INTO relationlog(id1,id2,relation,timestamp,node1,node2,remote) VALUES ",RelationB/binary>>),
							NRelationR = 0,
							NRelationB = <<>>;
						true ->
							NRelationR = RelationR,
							NRelationB = RelationB
					end,
					{ok,MyDB#tcp_mysql_db{logbuf = NLogB,bulk_log = NLogR,relationbuf = NRelationB,bulk_relation = NRelationR}};
				_ ->
					%% invalid log
					{ok,MyDB#tcp_mysql_db{logbuf = NLogB,bulk_log = NLogR}}
			end;
		_ ->
			%% normal log
			{ok,MyDB#tcp_mysql_db{logbuf = NLogB,bulk_log = NLogR}}
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

normalize_id(Raw) ->
	RawId = strip_raw(Raw),
	case binary:split(RawId,<<$,>>,[global]) of
		[Id] -> Id;
		Ids ->
			bcd2str(Ids,<<$">>,RawId)
	end.

bcd2str([],In,_Def) -> <<In/binary,$">>;
bcd2str([Dec|Rem],In,Def) ->
	case string:to_integer(binary_to_list(Dec)) of
		{error,_} -> Def;
		{I,_} ->
			I1 = (I bsr 4),
			I2 = (I band 15),
			if
				I1 > 9 ->
					A = I1 - 10 + $A;
				true ->
					A = I1 + $0
  			end,
			if
				I2 > 9 ->
					B = I2 + $A - 10;
				true ->
					B = I2 + $0
			end,
			bcd2str(Rem,<<In/binary,B,A>>,Def)
	end.
strip_raw(<<$<,$<,R/binary>>) -> R;
strip_raw(<<$[,R/binary>>) -> R;
strip_raw(R) -> R.

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

