# Transform SET (binary map representation) to string.
replicator.filter.settostring=com.continuent.tungsten.replicator.filter.SetToStringFilter
replicator.filter.settostring.url=jdbc:mysql:thin://${replicator.global.extract.db.host}:${replicator.global.extract.db.port}/${replicator.schema}?createDB=true
replicator.filter.settostring.user=${replicator.global.extract.db.user}
replicator.filter.settostring.password=${replicator.global.extract.db.password}