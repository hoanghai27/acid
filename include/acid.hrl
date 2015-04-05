
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

-record(acid_ftp_info,{
	id 		:: string(),	%% Host:Port:Filename
	size	:: integer(),	%% Last file size
	mtime	:: tuple()		%% Last modify time
}).

-endif.