replicator.applier.dbms.url=jdbc:vertica://${replicator.global.db.host}:${replicator.global.db.port}/@{REPL_VERTICA_DBNAME}
replicator.applier.dbms.user=${replicator.global.db.user}
replicator.applier.dbms.password=${replicator.global.db.password}
@{#(APPLIER.REPL_SVC_CONNECTION_INIT_SCRIPT)}replicator.applier.dbms.initScript=@{APPLIER.REPL_SVC_CONNECTION_INIT_SCRIPT}