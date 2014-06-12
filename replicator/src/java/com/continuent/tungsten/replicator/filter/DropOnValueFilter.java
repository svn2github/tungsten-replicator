/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2014 Continuent Inc.
 * Contact: tungsten@continuent.org
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of version 2 of the GNU General Public License as
 * published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA
 *
 * Initial developer(s): Linas Virbalas
 * Contributor(s): 
 */

package com.continuent.tungsten.replicator.filter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.conf.ReplicatorConf;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.RowChangeData.ActionType;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnSpec;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnVal;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * Filter which drops INSERTs when some column value is matched. Supports ROW
 * events only.
 * 
 * @author <a href="mailto:linas.virbalas@continuent.com">Linas Virbalas</a>
 * @version 1.0
 */
public class DropOnValueFilter implements Filter
{
    private static Logger     logger              = Logger.getLogger(DropOnValueFilter.class);

    /**
     * Path to definition file.
     */
    private String            definitionsFile     = null;

    /**
     * Parsed JSON holder.
     */
    private JSONArray         definitions         = null;

    /**
     * Count of value entries in the definitions file.
     */
    private int               definedValueEntries = 0;

    /**
     * Name of current replication service's internal tungsten schema.
     */
    private String            tungstenSchema;

    /**
     * Parser used to read column definition file.
     */
    private static JSONParser parser              = new JSONParser();

    /**
     * Sets the path to definition file.
     * 
     * @param definitionsFile Path to file.
     */
    public void setDefinitionsFile(String definitionsFile)
    {
        this.definitionsFile = definitionsFile;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.filter.Filter#filter(com.continuent.tungsten.replicator.event.ReplDBMSEvent)
     */
    public ReplDBMSEvent filter(ReplDBMSEvent event) throws ReplicatorException
    {
        ArrayList<DBMSData> data = event.getData();
        for (Iterator<DBMSData> itDBMSData = data.iterator(); itDBMSData
                .hasNext();)
        {
            DBMSData dataElem = itDBMSData.next();
            if (dataElem instanceof RowChangeData)
            {
                RowChangeData rdata = (RowChangeData) dataElem;

                filterEvent(event, rdata);

                // No more OneRowChange instances? Remove DBMSData.
                if (rdata.getRowChanges().isEmpty())
                    itDBMSData.remove();
            }
            else if (dataElem instanceof StatementData)
            {
                // Not supported.
            }
        }

        // No more data in this event? Remove event, but don't drop when dealing
        // with fragmented events (this could drop the commit part).
        if (event.getFragno() == 0 && event.getLastFrag() && data.isEmpty())
        {
            return null;
        }

        return event;
    }

    /**
     * Actual filtering logic.
     */
    public void filterEvent(ReplDBMSEvent event, RowChangeData rdata)
            throws ReplicatorException
    {
        for (Iterator<OneRowChange> itORC = rdata.getRowChanges().iterator(); itORC
                .hasNext();)
        {
            OneRowChange orc = itORC.next();

            // Skip any events that are not INSERTs.
            if (orc.getAction() != ActionType.INSERT)
                continue;

            // Don't analyze tables from Tungsten schema.
            if (orc.getSchemaName().compareToIgnoreCase(tungstenSchema) == 0)
            {
                if (logger.isDebugEnabled())
                    logger.debug("Ignoring " + tungstenSchema + " schema");
                continue;
            }

            // Check which values to filter.
            for (Object o : definitions)
            {
                JSONObject jo = (JSONObject) o;
                String defSchema = (String) jo.get("schema");
                String defTable = (String) jo.get("table");
                String defColumn = (String) jo.get("column");

                // Is there a filter request for this schema & table?
                if ((defSchema.equals("*") || defSchema.equals(orc
                        .getSchemaName()))
                        && (defTable.equals("*") || defTable.equals(orc
                                .getTableName())))
                {
                    // Column specifications and values.
                    ArrayList<ColumnSpec> colSpecs = orc.getColumnSpec();
                    ArrayList<ArrayList<OneRowChange.ColumnVal>> colValues = orc
                            .getColumnValues();

                    // Loop horizontally through columns.
                    for (int col = 0; col < colSpecs.size(); col++)
                    {
                        ColumnSpec colSpec = colSpecs.get(col);

                        // Check that column name is defined.
                        if (colSpec.getName() == null)
                        {
                            throw new ReplicatorException(
                                    "Expected to filter column, but column name is undefined: "
                                            + orc.getSchemaName() + "."
                                            + orc.getTableName() + "["
                                            + colSpec.getIndex() + "]");
                        }

                        // Is there a request for this column?
                        if (defColumn.equals("*")
                                || defColumn.equals(colSpec.getName()))
                        {
                            // Defined column values to filter.
                            JSONArray defValues = (JSONArray) jo.get("values");

                            // Iterate vertically rows of this column.
                            for (Iterator<ArrayList<ColumnVal>> itValuesInRow = colValues
                                    .iterator(); itValuesInRow.hasNext();)
                            {
                                ArrayList<ColumnVal> valuesInRow = itValuesInRow
                                        .next();
                                ColumnVal colValue = valuesInRow.get(col);

                                // Is there a request for this value?
                                if (colValue.getValue() != null
                                        && defValues.contains(colValue
                                                .getValue().toString()))
                                {
                                    // Drop this row.
                                    itValuesInRow.remove();

                                    logger.info(String
                                            .format("Row INSERT removed: seqno=%d schema=%s table=%s column=%s value=%s",
                                                    event.getSeqno(), orc
                                                            .getSchemaName(),
                                                    orc.getTableName(), colSpec
                                                            .getName(),
                                                    colValue.getValue()
                                                            .toString()));

                                    // No more rows? Remove ORC instance.
                                    if (colValues.size() == 0)
                                        itORC.remove();
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * Sets the Tungsten schema, which we ignore to prevent problems with the
     * replicator. This is mostly used for filter testing, which runs without a
     * pipeline.
     */
    public void setTungstenSchema(String tungstenSchema)
    {
        this.tungstenSchema = tungstenSchema;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException
    {
        if (tungstenSchema == null)
        {
            tungstenSchema = context.getReplicatorProperties().getString(
                    ReplicatorConf.METADATA_SCHEMA);
        }
        if (definitionsFile == null)
        {
            throw new ReplicatorException(
                    "definitionsFile property not set - specify a path to JSON file");
        }
    }

    /**
     * Returns how many column entries were parsed out of the JSON file.
     */
    public int getDefinedValueEntries()
    {
        return definedValueEntries;
    }

    /**
     * Initial validation of the JSON definitions file.
     */
    private void initDefinitionsFile() throws ReplicatorException
    {
        try
        {
            logger.info("Using: " + definitionsFile);

            String jsonText = NetworkClientFilter
                    .readDefinitionsFile(definitionsFile);
            Object obj = parser.parse(jsonText);
            JSONArray array = (JSONArray) obj;
            definitions = array;
            for (Object o : array)
            {
                JSONObject jo = (JSONObject) o;
                String schema = (String) jo.get("schema");
                String table = (String) jo.get("table");
                String column = (String) jo.get("column");
                JSONArray values = (JSONArray) jo.get("values");

                logger.info("  In " + schema + "." + table + "." + column
                        + ": ");

                if (values == null)
                    throw new ReplicatorException(
                            "JSON format incorrect: must defined \"values\" to filter");

                for (Object v : values)
                {
                    definedValueEntries++;
                    logger.info("    " + v);
                }
            }
        }
        catch (ClassCastException e)
        {
            throw new ReplicatorException(
                    "Unable to read definitions file (is JSON structure correct?): "
                            + e, e);
        }
        catch (ParseException e)
        {
            throw new ReplicatorException(
                    "Unable to read definitions file (error parsing JSON): "
                            + e, e);
        }
        catch (IOException e)
        {
            throw new ReplicatorException(e);
        }
    }

    /**
     * Prepares connection to the filtering server and parses definition file.
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context) throws ReplicatorException
    {
        initDefinitionsFile();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void release(PluginContext context) throws ReplicatorException
    {
        definitions = null;
        definedValueEntries = 0;
    }
}
