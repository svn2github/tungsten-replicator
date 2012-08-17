/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2011 Continuent Inc.
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
 * Initial developer(s): Jeff Mace
 */

package com.continuent.tungsten.replicator.filter;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnSpec;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.dbms.RowChangeData.ActionType;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * Filter to transform ROW INSERT/UPDATE changes into an audit table for 
 * a specified list of schema.table pairs.
 * 
 * @author <a href="mailto:jeff.mace@continuent.com">Jeff Mace</a>
 * @version 1.0
 * @see java.util.regex.Pattern
 * @see java.util.regex.Matcher
 */
public class BuildAuditTable implements Filter
{    
    private static Logger            logger = Logger.getLogger(BuildAuditTable.class);
    
    private String        targetSchemaName;
    
    public void setTargetSchemaName(String schemaName)
    {
        this.targetSchemaName = schemaName;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.filter.Filter#filter(com.continuent.tungsten.replicator.event.ReplDBMSEvent)
     */
    public ReplDBMSEvent filter(ReplDBMSEvent event) throws ReplicatorException
    {
        ArrayList<DBMSData> data = event.getData();
        
        if (data == null)
            return event;
            
        for (Iterator<DBMSData> eventIterator = data.iterator(); eventIterator.hasNext();)
        {
            DBMSData dataElem = eventIterator.next();
            if (dataElem instanceof RowChangeData)
            {
                RowChangeData rdata = (RowChangeData) dataElem;
                for (Iterator<OneRowChange> oneRowIterator = rdata.getRowChanges()
                        .iterator(); oneRowIterator.hasNext();)
                {
                    OneRowChange orc = oneRowIterator.next();
                    logger.debug("Parsing found schema = " + orc.getSchemaName()
                            + " / table = '" + orc.getTableName() + "'");
                    
                    if (orc.getAction() == ActionType.DELETE) {
                        logger.debug("Drop delete change");
                        oneRowIterator.remove();
                        continue;
                    }
                    
                    if (orc.getTableName().equalsIgnoreCase("names") != true) {
                        logger.debug("Drop change not on the names table");
                        oneRowIterator.remove();
                        continue;
                    }
                    orc.setTableName("names_log");
                    
                    if (orc.getAction() == ActionType.UPDATE) {
                        orc.setAction(ActionType.INSERT);
                    }
                    
                    orc.getKeySpec().clear();
                    orc.getKeyValues().clear();
                }
                if (rdata.getRowChanges().isEmpty())
                {
                    logger.debug("empty the eventIterator");
                    eventIterator.remove();
                }
            }
            else if (dataElem instanceof StatementData)
            {
                // Nothing
            }
        }
        
        // Don't drop events when dealing with fragmented events (This could
        // drop the commit part)
        if (event.getFragno() == 0 && event.getLastFrag() && data.isEmpty())
        {
            return null;
        }
        return event;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException
    {
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context) throws ReplicatorException
    {
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