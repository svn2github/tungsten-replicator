# HDFS data source. 
replicator.datasource.applier=com.continuent.tungsten.replicator.datasource.HdfsDataSource
replicator.datasource.applier.serviceName=${service.name}
# Storage location for replication catalog data. 
replicator.datasource.applier.directory=/user/tungsten/metadata

# HDFS-specific information. 
replicator.datasource.applier.hdfsUri=hdfs://@{APPLIER.REPL_DBHOST}:@{APPLIER.REPL_DBPORT}/user/tungsten/metadata
replicator.datasource.applier.hdfsConfigProperties=${replicator.home.dir}/conf/hdfs-config.properties

# CSV specification type.  This is the conventions for writing CSV files,
# which tend to be slightly different for each data source.  If set to 
# custom, use the custom CSV settings.  Other supported settings are 
# default, hive, etc.
replicator.datasource.applier.csvType=hive

# CSV type settings.  These are used if the csv type is custom.  The 
# The file and record separator values are congenial for Hive external 
# tables but it is simpler to use the hive csvType. 
replicator.datasource.applier.csv=com.continuent.tungsten.common.csv.CsvSpecification
replicator.datasource.applier.csv.fieldSeparator=\\u0001
replicator.datasource.applier.csv.RecordSeparator=\\n
replicator.datasource.applier.csv.useQuotes=false

# CSV data formatter.  This is the class responsible for translating 
# from Java objects to CSV strings.  The data format can vary independently
# from the CSV type based where data are extracted from or the types of
# tools that will process data. 
replicator.datasource.applier.csvFormatter=com.continuent.tungsten.replicator.csv.DefaultCsvDataFormat
