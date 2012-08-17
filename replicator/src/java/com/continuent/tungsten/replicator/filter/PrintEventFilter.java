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

import com.continuent.tungsten.commons.cache.IndexedLRUCache;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.conf.ReplicatorConf;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnSpec;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnVal;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.database.TableMatcher;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * Filter to transform a specific database name to a new value using Java
 * regular expression rules. This filter matches the schema name using the
 * fromRegex expression and then does a replacement on the name using the
 * toRegex expression.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 * @see java.util.regex.Pattern
 * @see java.util.regex.Matcher
 */
public class PrintEventFilter implements Filter
{    
    private static Logger            logger = Logger.getLogger(PrintEventFilter.class);

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.filter.Filter#filter(com.continuent.tungsten.replicator.event.ReplDBMSEvent)
     */
    public ReplDBMSEvent filter(ReplDBMSEvent event) throws ReplicatorException
    {
        ArrayList<DBMSData> data = event.getData();
        for (DBMSData dataElem : data)
        {
            if (dataElem instanceof RowChangeData)
            {
                RowChangeData rdata = (RowChangeData) dataElem;
                for (OneRowChange orc : rdata.getRowChanges())
                {
                    ArrayList<ColumnSpec> keys = orc.getKeySpec();
                    ArrayList<ColumnSpec> columns = orc.getColumnSpec();
                    ArrayList<ArrayList<ColumnVal>> keyValues = orc.getKeyValues();
                    ArrayList<ArrayList<ColumnVal>> columnValues = orc.getColumnValues();
                    
                    logger.info("Row event " + orc.getSchemaName() + "." + orc.getTableName());
                    
                    for (int r = 0; r < columnValues.size(); r++) {
                        logger.info("Row #" + r);
                        
                        try {
                            for (int k = 0; k < keys.size(); k++)
                            {
                                ColumnSpec spec = keys.get(k);
                                ColumnVal val = keyValues.get(r).get(k);
                                logger.info("Key " + spec.getName() + "=" + val.getValue());
                            }
                        } catch (java.lang.IndexOutOfBoundsException ioe) {
                        }

                        for (int c = 0; c < columns.size(); c++)
                        {
                            ColumnSpec spec = columns.get(c);
                            ColumnVal val = columnValues.get(r).get(c);
                            logger.info("Column " + spec.getName() + "=" + val.getValue());
                        }
                    }
                }
            }
            else if (dataElem instanceof StatementData)
            {
                StatementData sdata = (StatementData) dataElem;
                logger.info("Statement event " + sdata.getQuery());
            }
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