replicator.applier.oracle.service=@{APPLIER.REPL_ORACLE_SERVICE}
replicator.applier.oracle.sid=@{APPLIER.REPL_ORACLE_SID}
replicator.applier.dbms=com.continuent.tungsten.replicator.applier.OracleApplier
replicator.applier.dbms.dataSource=global
replicator.applier.dbms.getColumnMetadataFromDB=true
@{#(APPLIER.REPL_SVC_DATASOURCE_APPLIER_INIT_SCRIPT)}replicator.applier.dbms.initScript=@{APPLIER.REPL_SVC_DATASOURCE_APPLIER_INIT_SCRIPT}