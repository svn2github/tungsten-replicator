# Drops columns specified in definitions JSON file. For exact format see an example:
# samples/extensions/javascript/dropcolumn.json
replicator.filter.dropcolumn=com.continuent.tungsten.replicator.filter.JavaScriptFilter
replicator.filter.dropcolumn.script=${replicator.home.dir}/samples/extensions/javascript/dropcolumn.js
replicator.filter.dropcolumn.definitionsFile=~/dropcolumn.json