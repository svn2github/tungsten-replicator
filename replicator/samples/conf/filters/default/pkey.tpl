# Primary key filter.  This filter is required for MySQL row replication to 
# reduce the number of columns used in comparisons for updates and deletes. 
replicator.filter.pkey=com.continuent.tungsten.replicator.filter.PrimaryKeyFilter
replicator.filter.pkey.url=@{APPLIER.REPL_DBTHLURL}

# If left empty, credentials are taken from the JDBC connection.
replicator.filter.pkey.user=
replicator.filter.pkey.password=

# Set to true in order to add primary keys to INSERT operations.  This is
# required for batch loading. 
replicator.filter.pkey.addPkeyToInserts=false

# Set to true in order to add full column metadata to DELETEs.  This is
# likewise required for batch loading. 
replicator.filter.pkey.addColumnsToDeletes=false
