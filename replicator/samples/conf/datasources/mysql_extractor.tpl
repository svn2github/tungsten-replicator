# MySQL datasource used for extractor. 
replicator.datasource.extractor=com.continuent.tungsten.replicator.datasource.SqlDataSource

# Service name of the replicator.
replicator.datasource.extractor.serviceName=${service.name}

# Whether to create catalog tables. 
replicator.datasource.extractor.createCatalog=true

# Connection information for MySQL. 
replicator.datasource.extractor.connectionSpec=com.continuent.tungsten.replicator.datasource.SqlConnectionSpecMySQL
replicator.datasource.extractor.connectionSpec.host=@{EXTRACTOR.REPL_DBHOST}
replicator.datasource.extractor.connectionSpec.port=@{EXTRACTOR.REPL_DBPORT}
replicator.datasource.extractor.connectionSpec.user=@{EXTRACTOR.REPL_DBLOGIN}
replicator.datasource.extractor.connectionSpec.password=@{EXTRACTOR.REPL_DBPASSWORD}
replicator.datasource.extractor.connectionSpec.schema=${replicator.schema}

# CSV specification type.  This is the conventions for writing CSV files,
# which tend to be slightly different for each data source.  If set to 
# custom, use the custom CSV settings.  Other supported settings are 
# default, hive, etc.
replicator.datasource.extractor.csvType=mysql

# CSV data formatter.  This is the class responsible for translating 
# from Java objects to CSV strings.  The data format can vary independently
# from the CSV type based where data are extracted from or the types of
# tools that will process data. 
replicator.datasource.extractor.csvFormatter=com.continuent.tungsten.replicator.csv.DefaultCsvDataFormat
