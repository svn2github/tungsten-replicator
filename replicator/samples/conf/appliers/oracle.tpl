replicator.applier.oracle.service=@{APPLIER.REPL_ORACLE_SERVICE}
replicator.applier.oracle.sid=@{APPLIER.REPL_ORACLE_SID}
replicator.applier.dbms=com.continuent.tungsten.replicator.applier.OracleApplier
replicator.applier.dbms.url=@{APPLIER.REPL_DBJDBCURL}
replicator.applier.dbms.user=${replicator.global.db.user}
replicator.applier.dbms.password=${replicator.global.db.password}
#replicator.applier.dbms.maxSQLLogLength=3000
replicator.applier.dbms.getColumnMetadataFromDB=true