# Oracle data source. 
replicator.datasource.applier=com.continuent.tungsten.replicator.datasource.SqlDataSource

# Service name of the replicator.
replicator.datasource.applier.serviceName=${service.name}

# Connection information for Oracle.  This should be updated to use a class
# that specifically handles Oracle connection parameters such as SID. 
replicator.datasource.applier.connectionSpec=com.continuent.tungsten.replicator.datasource.SqlConnectionSpecGeneric
replicator.datasource.applier.connectionSpec.host=@{APPLIER.REPL_DBHOST}
replicator.datasource.applier.connectionSpec.port=@{APPLIER.REPL_DBPORT}
replicator.datasource.applier.connectionSpec.user=@{APPLIER.REPL_DBLOGIN}
replicator.datasource.applier.connectionSpec.password=@{APPLIER.REPL_DBPASSWORD}
replicator.datasource.applier.connectionSpec.schema=${replicator.schema}
replicator.datasource.applier.connectionSpec.url=@{APPLIER.REPL_DBJDBCURL}

# Number of channels for replication. 
replicator.datasource.applier.channels=${replicator.global.apply.channels}
