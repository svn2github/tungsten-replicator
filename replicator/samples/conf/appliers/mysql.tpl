# MySQL applier configuration. 
replicator.applier.dbms=com.continuent.tungsten.replicator.applier.MySQLDrizzleApplier
replicator.applier.dbms.dataSource=global
replicator.applier.dbms.ignoreSessionVars=autocommit
replicator.applier.dbms.getColumnMetadataFromDB=true
replicator.applier.optimizeRowEvents=false