
-ifndef(_ACID_HRL).
-define(_ACID_HRL,true).

-define(ACID_CONFIG_DB,acid_config_db).
-define(ACID_CAUSE_DB,acid_cause_db).
-define(ACID_FTP_SESSION,acid_ftp_session).
-define(ACID_FTP_INFO,acid_ftp_info).

-define(ACID_LOGGER_POOL,acid_logger_pool).
-define(ACID_TRACER_POOL,acid_tracer_pool).
-define(ACID_LOGGER_POOL_SIZE,5).
-define(BIN_DELIM,<<0,0>>).
-define(ACID_MAX_LOG_SIZE,10240).
-define(ACID_MAX_MOD_NAME,128).

-record(acid_ftp_info,{
	id 		:: string(),	%% Host:Port:Filename
	size	:: integer(),	%% Last file size
	mtime	:: tuple()		%% Last modify time
}).

-record(tcp_mysql_db,{
	bulk_size		::	integer(),		%% Bulk insert size
	bulk_log = 0	:: integer(),		%% Bulk number of msslog record
	bulk_cause = 0		:: integer(),	%% Bulk number of cause record
	bulk_relation = 0	:: integer(),	%% Bulk number of relation record
	logbuf	= <<>>	::	binary(),
	causebuf = <<>>	::	binary(),
	relationbuf = <<>>	:: binary() 
}).

-record(tcp_file,{
	fd					::	port(),			%% File descriptor
	size	= 0			::	integer(),		%% Current file size
	rsize	= 0			::	integer(),		%% File size for rotating
	date				::	string(),		%% Perodic for rolling
	sync_size = 65536	::	integer(),		%% File size for delaying write
	sync_interval = 5000::	timeout(),		%% Sync interval for delaying write
	inode 				::	integer(),		%% current inode
	check_interval = 2000	:: timeout(),	%% Check interval for rotate
	last_check = os:timestamp() :: erlang:timestamp(),
	nodename 			::	string(),		%% Node name
	fname				::	string()		%% Filename
}).

-endif.