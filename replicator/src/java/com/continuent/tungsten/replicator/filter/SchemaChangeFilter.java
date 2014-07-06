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
 * Initial developer(s): Robert Hodges
 * Contributor(s): Stephane Giron
 */

package com.continuent.tungsten.replicator.filter;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.conf.ReplicatorConf;
import com.continuent.tungsten.replicator.database.SqlOperation;
import com.continuent.tungsten.replicator.database.SqlStatementParser;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplOptionParams;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * Implements a filter to detect schema changes by parsing statements and create
 * permanent annotations on the statements. This filter also detects truncate
 * statements. Here is the general behavior.
 * <ol>
 * <li>Any statement that affects the logical schema of the table, such as
 * CREATE/DROP SCHEMA, CREATE/DROP TABLE, or ALTER causes the annotation
 * ##schema_change to be added to the event metadata.</li>
 * <li>Similarly any statement that is a table TRUNCATE operate causes the
 * annotation ##truncate to be added to the event metadata</li>
 * <li>In both cases the operation type, schema name, and table name (if
 * appropriate) are added to the statement metadata.</li>
 * </ol>
 */
public class SchemaChangeFilter implements Filter
{
    private static Logger            logger = Logger.getLogger(SchemaChangeFilter.class);

    private String                   tungstenSchema;
    private final SqlStatementParser parser = SqlStatementParser.getParser();

    /**
     * Sets the Tungsten schema which will be ignored if set.
     */
    public void setTungstenSchema(String tungstenSchema)
    {
        this.tungstenSchema = tungstenSchema;
    }

    /**
     * Checks transactions for statements that correspond to schema changes that
     * affect tables.
     * 
     * @see com.continuent.tungsten.replicator.filter.Filter#filter(com.continuent.tungsten.replicator.event.ReplDBMSEvent)
     */
    public ReplDBMSEvent filter(ReplDBMSEvent event)
            throws ReplicatorException, InterruptedException
    {
        // Assume we do not have any interesting operations to report.
        boolean schemaChange = false;
        boolean truncate = false;

        ArrayList<DBMSData> data = event.getData();
        String dbmsType;
        if (event.getDBMSEvent() == null)
            dbmsType = null;
        else
            dbmsType = event.getDBMSEvent().getMetadataOptionValue(
                    ReplOptionParams.DBMS_TYPE);

        if (data == null)
            return event;

        for (Iterator<DBMSData> iterator = data.iterator(); iterator.hasNext();)
        {
            DBMSData dataElem = iterator.next();
            if (dataElem instanceof RowChangeData)
            {
                // Nothing to be done.
            }
            else if (dataElem instanceof StatementData)
            {
                StatementData sdata = (StatementData) dataElem;
                String schema = null;
                String table = null;

                // Make a best effort to get parsing metadata on the statement.
                // Invoke parsing again if necessary.
                Object parsingMetadata = sdata.getParsingMetadata();
                if (parsingMetadata == null)
                {
                    String query = sdata.getQuery();
                    parsingMetadata = parser.parse(query, dbmsType);
                    sdata.setParsingMetadata(parsingMetadata);
                }

                // Usually we have parsing metadata at this point.
                SqlOperation parsed = null;
                if (parsingMetadata != null
                        && parsingMetadata instanceof SqlOperation)
                {
                    parsed = (SqlOperation) parsingMetadata;
                    schema = parsed.getSchema();
                    table = parsed.getName();
                }

                if (schema == null)
                    schema = sdata.getDefaultSchema();

                // Ensure we have enough to continue.
                if (parsed == null)
                {
                    logger.warn("Parsing failure: seqno=" + event.getSeqno());
                    continue;
                }
                else if (schema == null)
                {
                    final String query = sdata.getQuery();
                    logger.warn("Ignoring event : No schema found for this event "
                            + event.getSeqno()
                            + (query != null ? " ("
                                    + query.substring(0,
                                            Math.min(query.length(), 200))
                                    + "...)" : ""));
                    continue;
                }
                else if (schema.equals(tungstenSchema))
                {
                    if (logger.isDebugEnabled())
                        logger.debug("Ignoring change to tungsten schema: seqno="
                                + event.getSeqno());
                    continue;
                }

                // Check for an interesting schema change.
                int objectType = parsed.getObjectType();
                int operation = parsed.getOperation();
                if (objectType == SqlOperation.SCHEMA)
                {
                    if (operation == SqlOperation.CREATE)
                    {
                        annotate(sdata, schema, null, "CREATE SCHEMA");
                        schemaChange = true;
                    }
                    else if (operation == SqlOperation.DROP)
                    {
                        annotate(sdata, schema, null, "DROP SCHEMA");
                        schemaChange = true;
                    }
                }
                else if (objectType == SqlOperation.TABLE)
                {
                    if (operation == SqlOperation.CREATE)
                    {
                        annotate(sdata, schema, table, "CREATE TABLE");
                        schemaChange = true;
                    }
                    else if (operation == SqlOperation.DROP)
                    {
                        annotate(sdata, schema, table, "DROP TABLE");
                        schemaChange = true;
                    }
                    else if (operation == SqlOperation.ALTER)
                    {
                        annotate(sdata, schema, table, "ALTER TABLE");
                        schemaChange = true;
                    }
                    else if (operation == SqlOperation.RENAME)
                    {
                        annotate(sdata, schema, table, "RENAME TABLE");
                        schemaChange = true;
                    }
                    else if (operation == SqlOperation.TRUNCATE)
                    {
                        annotate(sdata, schema, table, "TRUNCATE");
                        truncate = true;
                    }
                }
            }
        }

        // Annotate events with schema change and truncate markers if such were
        // found.
        if (schemaChange)
        {
            event.getDBMSEvent().setMetaDataOption(
                    ReplOptionParams.SCHEMA_CHANGE, "");
        }
        if (truncate)
        {
            event.getDBMSEvent().setMetaDataOption(ReplOptionParams.TRUNCATE,
                    "");
        }

        return event;
    }

    /**
     * Annotates a statement with schema change information usable by downstream
     * filters.
     */
    private void annotate(StatementData sdata, String schema, String table,
            String operation)
    {
        sdata.setOption(ReplOptionParams.SCHEMA_NAME, schema);
        if (table != null)
            sdata.setOption(ReplOptionParams.TABLE_NAME, table);
        sdata.setOption(ReplOptionParams.OPERATION_NAME, operation);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        if (tungstenSchema == null && context != null)
        {
            tungstenSchema = context.getReplicatorProperties().getString(
                    ReplicatorConf.METADATA_SCHEMA);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void release(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
    }
}
