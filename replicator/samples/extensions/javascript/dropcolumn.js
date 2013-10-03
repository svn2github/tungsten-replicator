/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2013 Continuent Inc.
 * Contact: tungsten@continuent.org
 *
 * Filter removes specified columns from the THL row change events. Columns are defined in JSON file.
 *
 * Example of how to define one in static-<service>.properties:
 *
 * replicator.filter.dropColumn=com.continuent.tungsten.replicator.filter.JavaScriptFilter
 * replicator.filter.dropColumn.script=${replicator.home.dir}/samples/extensions/javascript/dropcolumn.js
 * replicator.filter.dropColumn.definitionsFile=~/dropcolumn.json
 *
 * See samples/extensions/javascript/dropcolumn.json for definition file example.
 *
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @author <a href="mailto:linas.virbalas@continuent.com">Linas Virbalas</a>
 */

any = new java.lang.String("*");

/**
 * Reads text file into string.
 */
function readFile(path)
{
  var file = new java.io.BufferedReader(new java.io.FileReader(new java.io.File(path)));

  var sb = new java.lang.StringBuffer();
  while((line = file.readLine()) != null)
  {
    sb.append(line);
    sb.append(java.lang.System.getProperty("line.separator"));
  }

  return sb.toString();
}

/**
 * Called once when JavaScriptFilter corresponding to this script is prepared.
 */
function prepare()
{
  definitionsFile = filterProperties.getString("definitionsFile");
  logger.info("dropcolumn.js using: " + definitionsFile);
  var json = readFile(definitionsFile);
  definitions = eval("(" + json + ")");
  
  logger.info("Columns to drop:");
  for (var i in definitions)
  {
    var drop = definitions[i];
    logger.info("In " + drop["schema"] + "." + drop["table"] + ": ");
    var cols = drop["columns"];
    for (var c in cols)
    {
      logger.info("  " + cols[c]);
    }
  }
}

function filter(event)
{
  data = event.getData();
  if(data != null)
  {
    for (i = 0; i < data.size(); i++)
    {
      d = data.get(i);
      if (d != null && d instanceof com.continuent.tungsten.replicator.dbms.RowChangeData)
      {
        filterRowChangeData(d);
      }
    }
  }
}

/**
 * Checks whether particular column should be dropped.
 */
function isDrop(schema, table, col)
{
  for (var def in definitions)
  {
    var drop = definitions[def];
    if(any.compareTo(drop["schema"]) == 0 || schema.compareTo(drop["schema"]) == 0)
    {
      if(any.compareTo(drop["table"]) == 0 || table.compareTo(drop["table"]) == 0)
      {
        var cols = drop["columns"];
        for (var cl in cols)
        {
          if(col == null)
          {
            throw new com.continuent.tungsten.replicator.ReplicatorException(
              "dropcolumn.js: column name in " + schema + "." + table +
              " is undefined - is colnames filter enabled and is it before the dropcolumn filter?"
              );
          }
          if(col.compareTo(cols[cl]) == 0)
          {
            return true;
          }
        }
      }
    }
  }
  return false;
}

function filterRowChangeData(d)
{
  rowChanges = d.getRowChanges();
  for(var j = 0; j < rowChanges.size(); j++)
  {
    oneRowChange = rowChanges.get(j);
    var schema = oneRowChange.getSchemaName();
    var table = oneRowChange.getTableName();
    var columns = oneRowChange.getColumnSpec();

    // Drop column values.    
    var columnValues = oneRowChange.getColumnValues();
    var specToDrop = new Array();
    for (var r = 0; r < columnValues.size(); r++)
    {
      for (c = columns.size() - 1 ; c >= 0 ; c--)
      {
        columnSpec = columns.get(c);
        colName = columnSpec.getName();
        if (isDrop(schema,table,colName))
        {
          columnValues.get(r).remove(c);
          if (specToDrop.indexOf(c) < 0)
            specToDrop[specToDrop.length] = c;
        }
      }
    }
    if (specToDrop.length > 0)
    {
      for (var i = 0 ; i < specToDrop.length ; i++)
      {
        columns.remove(specToDrop[i]);
      }
    }
    
    // Drop key values.
    var keyValues = oneRowChange.getKeyValues();
    var keys = oneRowChange.getKeySpec();
    var specToDrop = new Array();
    for (var r = 0; r < keyValues.size(); r++)
    {
      for (c = keys.size() - 1 ; c >= 0; c--)
      {
        keySpec = keys.get(c);
        colName = keySpec.getName();
        if (isDrop(schema,table,colName))
        {
          keyValues.get(r).remove(c);
          if (specToDrop.indexOf(c) < 0)
            specToDrop[specToDrop.length] = c;
        }
      }
    }
    if (specToDrop.length > 0)
    {
      for (var i = 0 ; i < specToDrop.length ; i++)
      {
        keys.remove(specToDrop[i]);
      }
    }
  }
}
