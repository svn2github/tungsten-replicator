# MongoDB data source.  This is a temporary hack and should be replaced by
# a proper MongoDB-specific implementation. 
replicator.datasource.applier=com.continuent.tungsten.replicator.datasource.FileDataSource
replicator.datasource.applier.serviceName=${service.name}

# Storage location for replication catalog data. 
replicator.datasource.applier.directory=${replicator.home.dir}/data
