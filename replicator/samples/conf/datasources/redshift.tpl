# Amazon Redshift datasource. 
replicator.datasource.applier=com.continuent.tungsten.replicator.datasource.SqlDataSource

# Service name of the replicator. 
replicator.datasource.applier.serviceName=${service.name}

# Connection information for Redshift. 
replicator.datasource.applier.connectionSpec=com.continuent.tungsten.replicator.datasource.SqlConnectionSpecRedshift
replicator.datasource.applier.connectionSpec.host=@{APPLIER.REPL_DBHOST}
replicator.datasource.applier.connectionSpec.port=@{APPLIER.REPL_DBPORT}
replicator.datasource.applier.connectionSpec.user=@{APPLIER.REPL_DBLOGIN}
replicator.datasource.applier.connectionSpec.password=@{APPLIER.REPL_DBPASSWORD}
replicator.datasource.applier.connectionSpec.databaseName=@{REPL_REDSHIFT_DBNAME}
replicator.datasource.applier.connectionSpec.schema=${replicator.schema}

# CSV specification type.  This is the conventions for writing CSV files,
# which tend to be slightly different for each data source.  If set to 
# custom, use the custom CSV settings.  Other supported settings are 
# default, hive, etc.
replicator.datasource.applier.csvType=redshift

# CSV data formatter.  This is the class responsible for translating 
# from Java objects to CSV strings.  The data format can vary independently
# from the CSV type based where data are extracted from or the types of
# tools that will process data. 
replicator.datasource.applier.csvFormatter=com.continuent.tungsten.replicator.csv.DefaultCsvDataFormat
