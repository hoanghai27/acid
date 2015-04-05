%% @author Tran Hoan <tvhoan88@gmail.com>
%% Created on 10:12:36 AM Feb 21, 2015
%% Purpose

%% maintainer


-module(acid_sup).
-behaviour(supervisor).
-export([init/1]).

-export([start_link/1]).

start_link(Args) ->
	supervisor:start_link({local,?MODULE},?MODULE,Args).

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
    Tcp = {acid_tcp_sup,{acid_tcp_sup,start_link,[]},
	      permanent,2000,worker,[acid_tcp_sup]},
    {ok,{{one_for_all,0,1}, [Tcp]}}.


