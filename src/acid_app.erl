%% @author Tran Hoan <tvhoan88@gmail.com>
%% Created on 5:50:53 PM Feb 17, 2015
%% Purpose

%% maintainer


-module(acid_app).
-behaviour(application).

-include("acid.hrl").

-export([start/2, stop/1]).

%% start/2
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/apps/kernel/application.html#Module:start-2">application:start/2</a>
-spec start(Type :: normal | {takeover, Node} | {failover, Node}, Args :: term()) ->
	{ok, Pid :: pid()}
	| {ok, Pid :: pid(), State :: term()}
	| {error, Reason :: term()}.
%% ====================================================================
start(_Type, StartArgs) ->
	application:set_env(lager,handlers,[{lager_console_backend, debug},
										{lager_file_backend,[{file,"log/" ++ atom_to_list(node()) ++ ".log"},
															 {level,debug}, {size, 10485760}, {date, "$D0"}, {count, 5}]}]),
	application:set_env(lager,crash_log,"log/" ++ atom_to_list(node()) ++ "_crash.log"),
	{ok,_} = application:ensure_all_started(lager),
	Conf = case os:getenv("ACID_CONFIG_FILE") of
			   false ->
				   case code:priv_dir(acid) of
					   {error,bad_name} ->
						   "./priv/acid.cfg";
					   N ->
						   N ++ "/acid.cfg"
				   end;
			   F ->
				   F
		   end,
	case file:consult(Conf) of
		{ok,Data} ->
			acid_api:init_db(),
			[acid_api:set_config(K, V) || {K,V} <- Data],
			TracerIp = acid_api:get_config(acid_tracer_ip),
			LoggerIp = acid_api:get_config(acid_logger_ip),
			TracerPort = acid_api:get_config(acid_tracer_port),
			LoggerPort = acid_api:get_config(acid_logger_port),
			case acid_api:get_config(acid_db_type,mysql) of
				mysql ->
					ok = application:start(crypto),
					ok = application:start(emysql),
					acid_api:connect_logger_db(mysql);
				_ ->
					ok
			end,
		    case acid_sup:start_link(StartArgs) of
				{ok, Pid} ->
					%% add acceptor
					lager:debug("add accepter for tracer ~p:~p",[TracerIp, TracerPort]),
					acid_tcp_sup:add_acceptor(tracer, TracerIp, TracerPort,[]),
					lager:debug("add accepter for logger ~p:~p",[LoggerIp, LoggerPort]),
					acid_tcp_sup:add_acceptor(logger, LoggerIp, LoggerPort,[]),
					{ok, Pid};
				Error ->
					Error
		    end;
		_ ->
			{error,bad_config}
	end.

%% stop/1
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/apps/kernel/application.html#Module:stop-1">application:stop/1</a>
-spec stop(State :: term()) ->  Any :: term().
%% ====================================================================
stop(_State) ->
    ok.

