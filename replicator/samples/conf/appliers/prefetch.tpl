# Prefetch applier.  This applier depends on a PrefetchStore to handle
# flow control on transactions that require prefetching. 
replicator.applier.dbms=com.continuent.tungsten.replicator.prefetch.PrefetchApplier

# URL, login, and password of Tungsten slave for which we are prefetching. 
# The URL must specify the replicator catalog schema name. 
replicator.applier.dbms.url=@{APPLIER.REPL_DBBASICJDBCURL}
replicator.applier.dbms.user=${replicator.global.db.user}
replicator.applier.dbms.password=${replicator.global.db.password}

# Slow query cache parameters.  Slow queries include those with large
# numbers of rows or poor selectivity, where selectivity is the
# fraction of the rows selected.  Any query that exceeds these is
# not repeated for the number of seconds in the cache duration property.  
replicator.applier.dbms.slowQueryCacheSize=10000
replicator.applier.dbms.slowQueryRows=1000
replicator.applier.dbms.slowQuerySelectivity=.05
replicator.applier.dbms.slowQueryCacheDuration=60
