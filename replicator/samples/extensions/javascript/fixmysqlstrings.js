/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2014 Continuent Inc.
 * Contact: tungsten@continuent.org
 *
 * JavaScript example for JavaScriptFilter.
 *
 * This filter fixes MySQL strings by converting byte values to either a 
 * normal Java String or a Hex'ed string if the source type is VARBINARY
 * or BINARY.  For this to work you must: 
 *
 *   1.) Run colnames filter before it in the filter chain. This filter 
 *       depends on the type description being filled in.  
 * 
 *   2.) Run before pkey.  The filter will break if pkey removes key columns.
 * 
 *   3.) Set the MySQL extractor to use bytes for strings, e.g.: 
 *       replicator.extractor.dbms.usingBytesForString=true
 *
 * @author <a href="mailto:eric.stone@continuent.com">Eric M. Stone</a>
 * @author <a href="mailto:robert.hodges@continuent.com">Robert M. Hodges</a>
 * @author <a href="mailto:linas.virbalas@continuent.com">Linas Virbalas</a>
 */

/**
 * Called once when JavaScriptFilter corresponding to this script is prepared.
 */
function prepare() {
  logger.info("fixMysqlStrings: Initializing...");
}

/**
 * Called on every filtered event. See replicator's javadoc for more details
 * on accessible classes. Also, JavaScriptFilter's javadoc contains description
 * about how to define a script like this.
 *
 * @param event Filtered com.continuent.tungsten.replicator.event.ReplDBMSEvent
 *
 * @see com.continuent.tungsten.replicator.filter.JavaScriptFilter
 * @see com.continuent.tungsten.replicator.event.ReplDBMSEvent
 * @see com.continuent.tungsten.replicator.dbms.DBMSData
 * @see com.continuent.tungsten.replicator.dbms.StatementData
 * @see com.continuent.tungsten.replicator.dbms.RowChangeData
 * @see com.continuent.tungsten.replicator.dbms.OneRowChange
 * @see com.continuent.tungsten.replicator.dbms.RowChangeData.ActionType
 * @see com.continuent.tungsten.replicator.thl.THLManagerCtrl#printRowChangeData(StringBuilder, RowChangeData, String, boolean, int)
 * @see java.lang.Thread
 * @see org.apache.log4j.Logger
 */
function filter(event) {
  // Get the data.
  data = event.getData();
  if (data != null) {
    // One ReplDBMSEvent may contain many DBMSData events.
    for (i = 0; i < data.size(); i++) {
      // Get com.continuent.tungsten.replicator.dbms.DBMSData
      d = data.get(i);

      // Determine the underlying type of DBMSData event.
      if (d != null && d instanceof com.continuent.tungsten.replicator.dbms.StatementData) {
        // We can't do anything about statements.
      } else if (d != null && d instanceof com.continuent.tungsten.replicator.dbms.RowChangeData) {
        processRowChanges(event, d);
      }
    }
  }
}

// Convert String bytes to blob or string based on type description. 
function processRowChanges(event, d) {
  rowChanges = d.getRowChanges();

  // One RowChangeData may contain many OneRowChange events.
  for (j = 0; j < rowChanges.size(); j++) {
    // Get com.continuent.tungsten.replicator.dbms.OneRowChange
    oneRowChange = rowChanges.get(j);
    var schema = oneRowChange.getSchemaName();
    var table = oneRowChange.getTableName();
    var columns = oneRowChange.getColumnSpec();
    var columnValues = oneRowChange.getColumnValues();
    fixUpStrings(schema, table, columns, columnValues);

    // Iterate through its keys if any
    keys = oneRowChange.getKeySpec();
    keyValues = oneRowChange.getKeyValues();
    fixUpStrings(schema, table, keys, keyValues);
  }
}

// Look for strings to fix up. 
function fixUpStrings(schema, table, columns, columnValues) 
{
  for (c = 0; c < columns.size(); c++) {
    columnSpec = columns.get(c);
    colName = columnSpec.getName();
    colType = columnSpec.getType();
    colDesc = columnSpec.getTypeDescription();

    // See if we have a string that needs sorting out.  12 is VARCHAR
    // from java.sql.Type.VARCHAR. 
    if (colType == 12) {
      logger.info("Found a VARCHAR column that may need sorting: column=" + colName + ' table=' + schema + '.' + table);
      // Iterate through the rows.
      for (row = 0; row < columnValues.size(); row++) {
        values = columnValues.get(row);
        value = values.get(c);
        raw_v = value.getValue();
        if (raw_v == null || colDesc == null) {
          logger.debug('value: NULL');
          // Do nothing
        } else {
          if (colDesc.startsWith("BINARY") || colDesc.startsWith("VARBINARY")) {
            // Convert to a hexadecimal string. 
            hex = javax.xml.bind.DatatypeConverter.printHexBinary(raw_v);
            value.setValue(hex);
          } 
          else {
            value.setValue(new java.lang.String(raw_v));
          }
        }
      }
    }
  }
}
