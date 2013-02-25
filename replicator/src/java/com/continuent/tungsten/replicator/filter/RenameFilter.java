/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2013 Continuent Inc.
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

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.conf.ReplicatorConf;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnSpec;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * Filter which renames schemas, tables and columns based on a file provided.
 * IMPORTANT: supports row change events only!
 * 
 * @author <a href="mailto:linas.virbalas@continuent.com">Linas Virbalas</a>
 * @version 1.0
 */
public class RenameFilter implements Filter
{
    private static Logger     logger = Logger.getLogger(RenameFilter.class);

    /**
     * Path to rename definition file.
     */
    private String            definitionsFile;

    /**
     * Name of current replication service's internal tungsten schema.
     */
    private String            tungstenSchema;

    /**
     * Requests for renaming.
     */
    private RenameDefinitions renameDefinitions;

    /**
     * Sets the path to rename definition file.
     * 
     * @param schemaNameSuffix Path to file.
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
        for (Iterator<DBMSData> iterator = data.iterator(); iterator.hasNext();)
        {
            DBMSData dataElem = iterator.next();
            if (dataElem instanceof RowChangeData)
            {
                RowChangeData rdata = (RowChangeData) dataElem;
                for (Iterator<OneRowChange> iterator2 = rdata.getRowChanges()
                        .iterator(); iterator2.hasNext();)
                {
                    OneRowChange orc = iterator2.next();

                    // Don't analyze tables from Tungsten schema.
                    if (orc.getSchemaName().compareToIgnoreCase(tungstenSchema) == 0)
                    {
                        if (logger.isDebugEnabled())
                            logger.debug("Ignoring " + tungstenSchema
                                    + " schema");
                        continue;
                    }

                    // Optimization: loop through column and key specifications
                    // only if there's a request to rename column for this
                    // schema and table.
                    if (renameDefinitions.shouldRenameColumn(
                            orc.getSchemaName(), orc.getTableName()))
                    {
                        // Rename column specs.
                        ArrayList<ColumnSpec> colSpecs = orc.getColumnSpec();
                        for (ColumnSpec colSpec : colSpecs)
                        {
                            if (colSpec.getName() != null)
                            {
                                String newColName = renameDefinitions
                                        .getNewColumnName(orc.getSchemaName(),
                                                orc.getTableName(),
                                                colSpec.getName());
                                if (newColName != null)
                                    colSpec.setName(newColName);
                            }
                            else
                            {
                                if (logger.isDebugEnabled())
                                {
                                    logger.debug("Expected to rename column, but original column name is undefined: "
                                            + orc.getSchemaName()
                                            + "."
                                            + orc.getTableName()
                                            + "["
                                            + colSpec.getIndex() + "]");
                                }
                            }
                        }

                        // Rename key specs.
                        ArrayList<ColumnSpec> keySpecs = orc.getKeySpec();
                        for (ColumnSpec keySpec : keySpecs)
                        {
                            if (keySpec.getName() != null)
                            {
                                String newColName = renameDefinitions
                                        .getNewColumnName(orc.getSchemaName(),
                                                orc.getTableName(),
                                                keySpec.getName());
                                if (newColName != null)
                                    keySpec.setName(newColName);
                            }
                            else
                            {
                                if (logger.isDebugEnabled())
                                {
                                    logger.debug("Expected to rename key, but original column name is undefined: "
                                            + orc.getSchemaName()
                                            + "."
                                            + orc.getTableName()
                                            + "["
                                            + keySpec.getIndex() + "]");
                                }
                            }
                        }
                    }

                    // Get new table name if there's a request.
                    String newTableName = renameDefinitions.getNewTableName(
                            orc.getSchemaName(), orc.getTableName());

                    // Get new schema name if there's a request.
                    String newSchemaName = renameDefinitions.getNewSchemaName(
                            orc.getSchemaName(), orc.getTableName());

                    // Finally, do the actual renaming.
                    if (newTableName != null)
                        orc.setTableName(newTableName);
                    if (newSchemaName != null)
                        orc.setSchemaName(newSchemaName);
                }
            }
        }
        return event;
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
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context) throws ReplicatorException
    {
        try
        {
            renameDefinitions = new RenameDefinitions(definitionsFile);
            renameDefinitions.parseFile();
        }
        catch (IOException e)
        {
            throw new ReplicatorException(e);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void release(PluginContext context) throws ReplicatorException
    {
    }
}
