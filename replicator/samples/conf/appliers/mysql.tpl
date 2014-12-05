# MySQL applier configuration. 
replicator.applier.dbms=com.continuent.tungsten.replicator.applier.MySQLDrizzleApplier
replicator.applier.dbms.dataSource=global
replicator.applier.dbms.ignoreSessionVars=autocommit
replicator.applier.dbms.getColumnMetadataFromDB=true
replicator.applier.dbms.optimizeRowEvents=@{REPL_SVC_APPLIER_OPTIMIZE_ROW_EVENTS}

# If true, similate time-zone unaware operation to process events from older
# Tungsten masters that do not extract events in a time zone-aware manner. 
# This option is only required when there is a chance of processing an older
# replicator log. 
replicator.applier.dbms.supportNonTzAwareMode=true
