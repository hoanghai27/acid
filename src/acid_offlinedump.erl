%% @author Tran Hoan <hoantv3@viettel.com.vn>
%% Copyright Softswitch Viettel  2012-2013. All Rights Reserved
%% Created on 1:30:09 PM Apr 2, 2015
%% Purpose dump erlang log and aricent FEP, BEP, EM

-module(acid_offlinedump).

-include("acid.hrl").
%% ====================================================================
%% API functions
%% ====================================================================
-export([ss7p_dump/1,
		 css7p_dump/3
		]).

-compile([export_all]).

%% get default 30 days ago
get_ftp_log(HostInfo,Pid) -> get_ftp_log(HostInfo,Pid,{day,30}).
get_ftp_log(HostInfo,Pid,Before) -> get_ftp_log(HostInfo,Pid,datetime_sub(dt_now(),Before),now).
get_ftp_log(HostInfo,Pid,From,To) when is_tuple(From) ->
	{ok,Result} = ftp:ls(Pid),
	Lines = string:tokens(Result,"\r\n"),
	CheckFunc = 
	fun(L) ->
			case string:tokens(L," ") of
				[[$-|_],_,_,_,_,_,_,_,Fn] ->
					case ftp:ls(Pid,Fn) of
						{ok,Result1} ->
							[L1] = string:tokens(Result1,"\r\n"),
							case string:tokens(L1," ") of
								[_,_,_,_,Size,M,D,HY,Fn] ->
									CT = dt_now(),
									MDT = case string:tokens(HY,":") of
											  [Y] ->
												  {{list_to_integer(Y),month2num(M),list_to_integer(D)},{0,0,0}};
											  [Hour,Min] ->
												  {{cyear(CT),month2num(M),list_to_integer(D)},{list_to_integer(Hour),list_to_integer(Min),0}}
										  end,
									if
										MDT >= From andalso (To == now orelse MDT =< To) ->
											MTimeEL = acid_api:get_config(acid_ss7p_logmtime,5),
											CheckDT = datetime_sub(CT, MTimeEL*60),
											case ftp:recv(Pid,Fn) of
												ok ->
													%% check for delete
													if CheckDT >= MDT ->
														   case ftp:delete(Pid,Fn) of
															   ok -> ok;
															   {error,R1} -> lager:error("rm ~p : ~p",[Fn,R1])
														   end,
														   acid_api:del_ftpinfo(HostInfo, Fn);
													   true ->
														   case acid_api:get_ftpinfo(HostInfo, Fn) of
															   #acid_ftp_info{size = Size} ->
																   case ftp:delete(Pid,Fn) of
																	   ok -> ok;
																	   {error,R1} -> lager:error("rm ~p : ~p",[Fn,R1])
																   end,
																   acid_api:del_ftpinfo(HostInfo, Fn);
															   _ ->
																   acid_api:set_ftpinfo(HostInfo, Fn, Size, MDT)
														   end
													end;
												{error,R2} ->
													lager:error("download ~p : ~p",[Fn,R2])
											end;
										true ->
											lager:debug("ignore too old or too new ~p",[Fn])
									end;
								_ ->
									lager:error("unkown ls -l ~p output",[Fn])
							end;
						_ ->
							lager:error("Oops ~p may be deleted or moved",[Fn])
					end;
				[_,_,_,_,_,_,_,_,Fn] ->
					lager:debug("ignore directory or symbolic link ~p",[Fn]);
				Exp ->
					lager:error("unkown ls -l result ~p",[Exp])
			end
	end,
	[CheckFunc(X) || X <- Lines],
	ok.

ss7p_dump(all) ->
	StickDir = acid_api:get_config(acid_ss7p_stickdir,"/opt/mssng/log/stick"),
	SpoolDir = acid_api:get_config(acid_ss7p_spooldir,"/opt/mssng/log/spool"),
	Func = fun(E,SpoolDir) ->
				   SDir = SpoolDir ++ "/" ++ extract_date(E),
				   case is_dump(E) of
					   true ->
						   %% spawn 
						   spawn(?MODULE,css7p_dump,[self(),E,SDir]);
					   _ ->
						   spool_file(E,SDir)
				   end,
				   SpoolDir
		   end,
	filelib:fold_files(StickDir,".*",true,Func,SpoolDir);

ss7p_dump(Fn) when is_list(Fn) ->
	try
		{ok,IO} = file:open(Fn,[read,binary]),
		{{D,T},Node,Pid} = ss7p_dump_hdr(Fn,IO),
		BulkSize = acid_api:get_config(acid_db_bulk,10),
		ss7p_dump_body(IO,<<"INFO">>,date2string(D),Node,Pid,time2string(T),<<>>,BulkSize,0,<<>>),
		file:close(IO)
	catch
		_:_ ->
			lager:error("open failed : ~p",[erlang:get_stacktrace()])
	end.

css7p_dump(Parent,Fn,SDir) ->
	try
		{ok,IO} = file:open(Fn,[read,binary]),
		{{D,T},Node,Pid} = ss7p_dump_hdr(Fn,IO),
		BulkSize = acid_api:get_config(acid_db_bulk,10),
		ss7p_dump_body(IO,<<"INFO">>,date2string(D),Node,Pid,time2string(T),<<>>,BulkSize,0,<<>>),
		file:close(IO),
		spool_file(Fn,SDir),
		Parent ! {dumped,Fn}
	catch
		_:_ ->
			lager:error("open failed : ~p",[erlang:get_stacktrace()]),
			Parent ! {dump_error,Fn}
	end.

ss7p_dump_body(IO,Level,D,Node,Pid,LTime,LBuf,BulkSize,BulkNum,Bulk) ->
	case file:read_line(IO) of
		{ok,<<$[,_/binary>> = L} ->
			{NBN,NB} = ss7p_write_db(<<D/binary," ",LTime/binary>>,Level,Node,Pid,LBuf,BulkSize,BulkNum,Bulk),
			case binary:split(L,<<$]>>) of
				[H,T] ->
					NLevel = case binary:split(T,<<$:>>) of
								 [<<" ",Level1/binary>>|_] -> 
									 if size(Level1) < 20 -> Level1;
									 	true -> <<"DEBUG">>
									 end;
								 [Level1|_] -> Level1;
								 _ -> <<"DEBUG">>
							 end,
					case binary:split(H,<<" ">>,[global]) of
						[<<$[>>,_,_,NTime|_] ->
							ss7p_dump_body(IO,NLevel,D,Node,Pid,NTime,L,BulkSize,NBN,NB);
						E1 ->
							lager:error("strange time format ~p",[E1]),
							ss7p_dump_body(IO,Level,D,Node,Pid,LTime,<<LBuf/binary,L/binary>>,BulkSize,NBN,NB)
					end;
				E ->
					lager:error("strange ss7p log line ~p",[E]),
					ss7p_dump_body(IO,Level,D,Node,Pid,LTime,<<LBuf/binary,L/binary>>,BulkSize,NBN,NB)
			end;
		{ok,L} ->
			ss7p_dump_body(IO,Level,D,Node,Pid,LTime,<<LBuf/binary,L/binary>>,BulkSize,BulkNum,Bulk);
		_ ->
			if
				size(LBuf) > 0 ->
					{_,NB} = ss7p_write_db(<<D/binary," ",LTime/binary>>,Level,Node,Pid,LBuf,BulkSize,BulkNum,Bulk),
					MetaSQL = <<"INSERT INTO ss7plog(logtime,loglevel,node,pid,msg) VALUES ",NB/binary>>,
					emysql:execute(?ACID_LOGGER_POOL,MetaSQL);
				true ->
					ignore
			end,
			eof
	end.

ss7p_dump_hdr(Fn,IO) ->
	Pid = case string:tokens(Fn,"-") of
			  [_,P|_] ->	iolist_to_binary(P);
			  _ ->			<<"NULL">>
		  end,
	{ok,TSbin} = file:read_line(IO),
	DateTime = epoch2datetime(TSbin),
	%% remove #################
	{ok,_} = file:read_line(IO),
	{ok,FName} = file:read_line(IO),
	%% remove #################
	{ok,_} = file:read_line(IO),
	{ok,_} = file:read_line(IO),
	case binary:split(FName,<<" ">>) of
		[<<"LOGS_FEP_Inst_",ID/binary>>|_] ->
			{DateTime,<<"FEP",ID/binary>>,Pid};
		[<<"LOGS_BEP_Inst_",ID/binary>>|_] ->
			{DateTime,<<"BEP",ID/binary>>,Pid};
		[<<"LOGS_EM">>|_] ->
			{DateTime,<<"EM">>,Pid};
		_ ->
			file:close(IO)		
	end.

ss7p_seek(IO,0) ->
	IO;
ss7p_seek(IO,Line) when Line > 0 ->
	case file:read_line(IO) of
		eof -> IO;
		{error,_} -> IO;
		{ok,_} -> ss7p_seek(IO,Line - 1)
	end.

ss7p_write_db(TS,Level,Node,Pid,Msg,BulkSize,BulkNum,Bulk) ->
	NB = if
			 size(Bulk) == 0 ->
				 <<"('",TS/binary,$',$,,$',Level/binary,$',$,,$',Node/binary,$',$,,$',Pid/binary,$',$,,$',Msg/binary,"')">>;
			 true ->
				 <<Bulk/binary,",","('",TS/binary,$',$,,$',Level/binary,$',$,,$',Node/binary,$',$,,$',Pid/binary,$',$,,$',Msg/binary,"')">>
		 end,
	NBN = BulkNum + 1,
	if
		NBN >= BulkSize ->
			MetaSQL = <<"INSERT INTO ss7plog(logtime,loglevel,node,pid,msg) VALUES ",NB/binary>>,
			emysql:execute(?ACID_LOGGER_POOL,MetaSQL),
			{0,<<>>};
		true ->
			{NBN,NB}
	end.

epoch2datetime(S) when is_binary(S) -> epoch2datetime(string:strip(binary_to_list(S),both,$\n));
epoch2datetime(S) when is_list(S) ->
	case string:to_integer(S) of
		{error,_} ->
			erlang:localtime();
		{Int,_} ->
			epoch2datetime(Int)
	end;
epoch2datetime(Int) when is_integer(Int) ->
	C = calendar:datetime_to_gregorian_seconds({{1970,1,1},{0,0,0}}) + Int,
	calendar:gregorian_seconds_to_datetime(C).
date2string({Y,M,D}) ->
	iolist_to_binary(lists:flatten(io_lib:format("~4.10.0B-~2.10.0B-~2.10.0B",[Y,M,D]))).
time2string({H,M,S}) ->
	iolist_to_binary(lists:flatten(io_lib:format("~2.10.0B.~2.10.0B.~2.10.0B",[H,M,S])) ++ ".000").
datetime_sub(DateTime,{day,D}) -> datetime_sub(DateTime,D*86400);
datetime_sub(DateTime,{hour,H}) -> datetime_sub(DateTime,H*3600);
datetime_sub(DateTime,Secs) when is_integer(Secs) ->
	calendar:gregorian_seconds_to_datetime(calendar:datetime_to_gregorian_seconds(DateTime) - Secs).
month2num("Jan") -> 1;
month2num("Feb") -> 2;
month2num("Mar") -> 3;
month2num("Apr") -> 4;
month2num("May") -> 5;
month2num("Jun") -> 6;
month2num("Jul") -> 7;
month2num("Aug") -> 8;
month2num("Sep") -> 9;
month2num("Oct") -> 10;
month2num("Nov") -> 11;
month2num("Dec") -> 12.
cyear({{CY,_,_},_}) -> CY.
dt_now() -> 
	[DT] = calendar: local_time_to_universal_time_dst(calendar:local_time()),
	DT.

is_dump(Fn) ->
	case string:str(Fn,"LOGS_") of
		0 -> false;
		_ -> true
	end.
extract_date(Fn) ->
	BFn = filename:basename(Fn),
	case string:str(BFn,"LOGS_") of
		0 ->
			case string:tokens(BFn,"_") of
				["ss7p","events",_,M,D|_] ->
					CY = cyear(dt_now()),
					lists:flatten(io_lib:format("~4.10.0B-~2.10.0B-",[CY,month2num(M)])) ++ D;
				["SPL",_,_,Y,M,D|_] ->
					binary_to_list(date2string({list_to_integer(Y),list_to_integer(M),list_to_integer(D)}));
				_ ->
					binary_to_list(date2string(erlang:date()))
			end;
		_ ->
			case string:tokens(BFn,"-") of
				[_,_,Ds] ->
					case string:tokens(Ds,"_") of
						[M,D|_] ->
							CY = cyear(dt_now()),
							lists:flatten(io_lib:format("~4.10.0B-~2.10.0B-",[CY,month2num(M)])) ++ D;
						_ ->
							binary_to_list(date2string(erlang:date()))
					end;
				_ ->
					binary_to_list(date2string(erlang:date()))
			end
	end.

spool_file(E,SDir) ->
	case file:make_dir(SDir) of
	   ok ->
		   case file:rename(E,SDir ++ "/" ++ filename:basename(E)) of
			   ok -> ok;
			   {error,R1} ->
				   lager:error("move ~p to ~p : ~p",[E,SDir,R1])
		   end;
	   {error,eexist} ->
		   case file:rename(E,SDir ++ "/" ++ filename:basename(E)) of
			   ok -> ok;
			   {error,R1} ->
				   lager:error("move ~p to ~p : ~p",[E,SDir,R1])
		   end;
	   {error,R2} ->
		   lager:error("mkdir : ~p",[R2])
   end.

test() ->
	{ok,A} = ftp:open("10.60.3.133",[{port,8989}]),
	ftp:user(A,"msc","msc@2013"),
	ok = ftp:cd(A,"/opt/mssng/log/sgw/vmsc"),
	ok = ftp:lcd(A,acid_api:get_config(acid_ss7p_stickdir,"/opt/mssng/log/stick")),
	HostInfo = "10.60.3.133:8989",
	get_ftp_log(HostInfo,A),
	ftp:close(A).