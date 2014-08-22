# Oracle data source. 
replicator.datasource.global=com.continuent.tungsten.replicator.datasource.SqlDataSource

# Service name of the replicator.
replicator.datasource.global.serviceName=${service.name}

# Connection information for Oracle.  This should be updated to use a class
# that specifically handles Oracle connection parameters such as SID. 
replicator.datasource.global.connectionSpec=com.continuent.tungsten.replicator.datasource.SqlConnectionSpecGeneric
replicator.datasource.global.connectionSpec.host=@{APPLIER.REPL_DBHOST}
replicator.datasource.global.connectionSpec.port=@{APPLIER.REPL_DBPORT}
replicator.datasource.global.connectionSpec.user=@{APPLIER.REPL_DBLOGIN}
replicator.datasource.global.connectionSpec.password=@{APPLIER.REPL_DBPASSWORD}
replicator.datasource.global.connectionSpec.schema=${replicator.schema}
replicator.datasource.global.connectionSpec.url=@{APPLIER.REPL_DBJDBCURL}
