========================================================
Deploy acid
========================================================

edit acid.cfg in priv

cd path_to_acid

erl -pa ebin -pa path_to_lager/ebin -pa path_to_emysql/ebin -setcookie COOKIE -name acid@x.x.x.x -noinput -detached -eval "application:start(acid)"
erl -pa ebin -pa /opt/mssng/lager/ebin -pa path_to_emysql/ebin -setcookie 123 -name acid@10.61.64.47 -noinput -detached -eval "application:start(acid)"
 

=========================================================
Test
=========================================================
	lager_acid_backend:test_init().
	lager_acid_backend:test_log("tran van hoàn").