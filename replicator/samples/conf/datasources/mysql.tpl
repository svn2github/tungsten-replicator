# MySQL datasource. 
replicator.datasource.applier=com.continuent.tungsten.replicator.datasource.SqlDataSource

# Standard location information for a relational DBMS.  
replicator.datasource.applier.serviceName=${service.name}
replicator.datasource.applier.host=@{APPLIER.REPL_DBHOST}
replicator.datasource.applier.port=@{APPLIER.REPL_DBPORT}
replicator.datasource.applier.user=@{APPLIER.REPL_DBLOGIN}
replicator.datasource.applier.password=@{APPLIER.REPL_DBPASSWORD}

# Schema used to store catalog tables. 
replicator.datasource.applier.schema=${replicator.schema}

# MySQL-specific JDBC URL for drizzle JDBC driver. 
replicator.datasource.applier.url=jdbc:mysql:thin://${replicator.datasource.applier.host}:${replicator.datasource.applier.port}/${replicator.schema}

# CSV specification type.  This is the conventions for writing CSV files,
# which tend to be slightly different for each data source.  If set to 
# custom, use the custom CSV settings.  Other supported settings are 
# default, hive, etc.
replicator.datasource.applier.csvType=custom

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
