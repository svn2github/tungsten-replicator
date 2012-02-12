# Prefetch applier.  This applier depends on a PrefetchStore to handle
# flow control on transactions that require prefetching. 
replicator.applier.dbms=com.continuent.tungsten.replicator.prefetch.PrefetchApplier

# URL, login, and password of Tungsten slave for which we are prefetching. 
# The URL must specify the replicator catalog schema name. 
replicator.applier.dbms.url=@{APPLIER.REPL_DBBASICJDBCURL}
replicator.applier.dbms.user=${replicator.global.db.user}
replicator.applier.dbms.password=${replicator.global.db.password}
