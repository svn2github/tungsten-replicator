replicator.applier.dbms=com.continuent.tungsten.replicator.applier.PostgreSQLApplier
replicator.applier.dbms.url=jdbc:postgresql://${replicator.global.db.host}:${replicator.global.db.port}/@{REPL_POSTGRESQL_DBNAME}
replicator.applier.dbms.user=${replicator.global.db.user}
replicator.applier.dbms.password=${replicator.global.db.password}
replicator.applier.dbms.getColumnMetadataFromDB=true