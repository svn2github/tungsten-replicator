/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2013 Continuent Inc.
 * Contact: tungsten@continuent.org
 *
 * replicator.filter.dropColumn=com.continuent.tungsten.replicator.filter.JavaScriptFilter
 * replicator.filter.dropColumn.script=../samples/extensions/javascript/dropColumn.js
 * replicator.filter.dropColumn.columns=#column1#column2#
 *
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 */

/**
 * Called once when JavaScriptFilter corresponding to this script is prepared.
 */
function prepare()
{
  colsToDrop = filterProperties.getString("columns").toUpperCase();
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

function filterRowChangeData(d)
{
  rowChanges = d.getRowChanges();
  for(j = 0; j < rowChanges.size(); j++)
  {
    oneRowChange = rowChanges.get(j);
    columns = oneRowChange.getColumnSpec();
    var columnValues = oneRowChange.getColumnValues();

    specToDrop = new Array();
    for (r = 0; r < columnValues.size(); r++)
    {
      for (c = columns.size() - 1 ; c >= 0 ; c--)
      {
        columnSpec = columns.get(c);
        colName = columnSpec.getName().toUpperCase();
        if (colsToDrop.indexOf("#"+colName+"#")>=0)
        {
          columnValues.get(r).remove(c);
          if (specToDrop.indexOf(c) < 0)
            specToDrop[specToDrop.length] = c;
        }
      }
    }
    if(specToDrop.length > 0)
    {
      for ( i = 0 ; i < specToDrop.length ; i++)
      {
        columns.remove(specToDrop[i]);
      }
    }
    
    var keyValues = oneRowChange.getKeyValues();
    keys = oneRowChange.getKeySpec();
    specToDrop = new Array();

    for (r = 0; r < keyValues.size(); r++)
    {
      for (c = keys.size() - 1 ; c >= 0; c--)
      {
        keySpec = keys.get(c);
        colName = keySpec.getName().toUpperCase();
        if (colsToDrop.indexOf("#"+colName+"#")>=0)
        {
          keyValues.get(r).remove(c);
          if (specToDrop.indexOf(c) < 0)
            specToDrop[specToDrop.length] = c;
        }
      }
    }
    if(specToDrop.length > 0)
    {
      for ( i = 0 ; i < specToDrop.length ; i++)
      {
        keys.remove(specToDrop[i]);
      }
    }
  }
}
