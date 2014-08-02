# MySQL datasource. 
replicator.datasource.applier=com.continuent.tungsten.replicator.datasource.SqlDataSource

# Service name of the replicator.
replicator.datasource.applier.serviceName=${service.name}

# Connection information for MySQL. 
replicator.datasource.applier.connectionSpec=com.continuent.tungsten.replicator.datasource.SqlConnectionSpecMySQL
replicator.datasource.applier.connectionSpec.host=@{APPLIER.REPL_DBHOST}
replicator.datasource.applier.connectionSpec.port=@{APPLIER.REPL_DBPORT}
replicator.datasource.applier.connectionSpec.user=@{APPLIER.REPL_DBLOGIN}
replicator.datasource.applier.connectionSpec.password=@{APPLIER.REPL_DBPASSWORD}
replicator.datasource.applier.connectionSpec.schema=${replicator.schema}
replicator.datasource.applier.connectionSpec.privilegedSlaveUpdate=${replicator.log.slave.updates}
replicator.datasource.applier.connectionSpec.logSlaveUpdates=${replicator.schema}

# Number of channels for replication. 
replicator.datasource.applier.channels=${replicator.global.apply.channels}

# CSV specification type.  This is the conventions for writing CSV files,
# which tend to be slightly different for each data source.  If set to 
# custom, use the custom CSV settings.  Other supported settings are 
# default, hive, etc.
replicator.datasource.applier.csvType=mysql

# CSV type settings.  These are used if the csv type is custom.
replicator.datasource.applier.csv=com.continuent.tungsten.common.csv.CsvSpecification
replicator.datasource.applier.csv.fieldSeparator=,
replicator.datasource.applier.csv.RecordSeparator=\\n
replicator.datasource.applier.csv.nullValue=\\N
replicator.datasource.applier.csv.useQuotes=true

# CSV data formatter.  This is the class responsible for translating 
# from Java objects to CSV strings.  The data format can vary independently
# from the CSV type based where data are extracted from or the types of
# tools that will process data. 
replicator.datasource.applier.csvFormatter=com.continuent.tungsten.replicator.csv.DefaultCsvDataFormat
