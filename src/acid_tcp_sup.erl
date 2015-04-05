%% @author Tran Hoan <tvhoan88@gmail.com>
%% Created on 5:50:05 PM Feb 17, 2015
%% Purpose

%% maintainer

-module(acid_tcp_sup).
-behaviour(supervisor).
-export([init/1]).

-export([start_link/0,
		 add_acceptor/4,
		 add_socket/3
]).

start_link() ->
	supervisor:start_link({local,?MODULE},?MODULE,[]).

%% init/1
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/supervisor.html#Module:init-1">supervisor:init/1</a>
-spec init(Args :: term()) -> Result when
	Result :: {ok, {SupervisionPolicy, [ChildSpec]}} | ignore,
	SupervisionPolicy :: {RestartStrategy, MaxR :: non_neg_integer(), MaxT :: pos_integer()},
	RestartStrategy :: one_for_all
					 | one_for_one
					 | rest_for_one
					 | simple_one_for_one,
	ChildSpec :: {Id :: term(), StartFunc, RestartPolicy, Type :: worker | supervisor, Modules},
	StartFunc :: {M :: module(), F :: atom(), A :: [term()] | undefined},
	RestartPolicy :: permanent
				   | transient
				   | temporary,
	Modules :: [module()] | dynamic.
%% ====================================================================
init(_Args) ->
    {ok,{{one_for_one,0,1}, []}}.

add_acceptor(Type,Ips,Port,Opts) ->
	Spec = {Port,{acid_tcp_acceptor,start_link,[{Type,Ips,Port,Opts}]},permanent,1000,worker,[acid_tcp_acceptor]},
	supervisor:start_child(?MODULE,Spec).

add_socket(Type,Fd,Opts) ->
	Spec = {Fd,{acid_tcp_socket,start_link,[{Type,Fd,Opts}]},temporary,1000,worker,[acid_tcp_socket]},
	supervisor:start_child(?MODULE,Spec).



