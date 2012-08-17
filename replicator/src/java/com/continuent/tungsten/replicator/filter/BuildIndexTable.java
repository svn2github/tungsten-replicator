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
public class BuildIndexTable implements Filter
{    
    private static Logger            logger = Logger.getLogger(BuildIndexTable.class);
    
    private String                   targetSchemaName;
    
    private String                   tungstenSchema;
    
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
        for (DBMSData dataElem : data)
        {
            if (dataElem instanceof RowChangeData)
            {
                RowChangeData rdata = (RowChangeData) dataElem;
                for (OneRowChange orc : rdata.getRowChanges())
                {
                    // Tungsten schema is always passed through as dropping this can
                    // confuse the replicator.
                    if (orc.getSchemaName().equals(tungstenSchema)) {
                        continue;
                    }
                        
                    ArrayList<ColumnSpec> keys = orc.getKeySpec();
                    ArrayList<ColumnSpec> columns = orc.getColumnSpec();
                    ArrayList<ArrayList<ColumnVal>> keyValues = orc.getKeyValues();
                    ArrayList<ArrayList<ColumnVal>> columnValues = orc.getColumnValues();
                    
                    OneRowChange.ColumnSpec c = orc.new ColumnSpec();
                    c.setType(12);
                    c.setName("schema");
                    
                    OneRowChange.ColumnVal v = orc.new ColumnVal();
                    v.setValue(orc.getSchemaName());
                    
                    columns.add(c);
                    keys.add(c);
                    
                    for (ArrayList<OneRowChange.ColumnVal> cValues : columnValues)
                    {
                        cValues.add(v);
                    }
                    
                    for (ArrayList<OneRowChange.ColumnVal> kValues : keyValues)
                    {
                        kValues.add(v);
                    }
                    
                    orc.setSchemaName(this.targetSchemaName);
                }
            }
            else if (dataElem instanceof StatementData)
            {
                // Nothing
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
        tungstenSchema = context.getReplicatorProperties().getString(
                ReplicatorConf.METADATA_SCHEMA);
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