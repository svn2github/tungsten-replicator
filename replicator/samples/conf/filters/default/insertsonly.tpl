# Remove Row-Based Replication events that aren't INSERT
replicator.filter.insertsonly=com.continuent.tungsten.replicator.filter.JavaScriptFilter                           
replicator.filter.insertsonly.script=${replicator.home.dir}/samples/extensions/javascript/insertsonly.js