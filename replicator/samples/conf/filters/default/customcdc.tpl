# Filter which for each transaction adds a change data capture (CDC) row to the
# change table, identified by suffix.

replicator.filter.customcdc=com.continuent.tungsten.replicator.filter.CDCMetadataFilter
replicator.filter.customcdc.schemaNameSuffix=
replicator.filter.customcdc.tableNameSuffix=