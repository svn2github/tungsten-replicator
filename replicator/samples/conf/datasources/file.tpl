# File datasource. 
replicator.datasource.applier=com.continuent.tungsten.replicator.datasource.FileDataSource
replicator.datasource.applier.serviceName=${service.name}

# Storage location for replication catalog data. 
replicator.datasource.applier.directory=${replicator.home.dir}/data

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
replicator.datasource.applier.csv.useHeaders=false

# CSV data formatter.  This is the class responsible for translating
# from Java objects to CSV strings.  The data format can vary independently
# from the CSV type based where data are extracted from or the types of 
# tools that will process data. 
replicator.datasource.applier.csvFormatter=com.continuent.tungsten.replicator.csv.DefaultCsvDataFormat
