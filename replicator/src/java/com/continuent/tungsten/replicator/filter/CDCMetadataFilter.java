/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2012 Continuent Inc.
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
 * Initial developer(s): Stephane Giron
 * Contributor(s): Linas Virbalas
 */

package com.continuent.tungsten.replicator.filter;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.conf.ReplicatorConf;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.DatabaseFactory;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnSpec;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnVal;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.dbms.RowChangeData.ActionType;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * Filter which for each row change transaction adds a change data capture row
 * to a corresponding change table.<br/>
 * <br/>
 * Change table structure:<br/>
 * original columns ..., CDC_OP_TYPE, CDC_TIMESTAMP, CDC_SEQUENCE_NUMBER<br/>
 * <br/>
 * Filter automatically generates a CDC_SEQUENCE_NUMBER primary key value for
 * the change table. However, for this to work correctly, caller must ensure
 * there are no parallel applies to the same change table (normally it isn't a
 * problem).
 * 
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class CDCMetadataFilter implements Filter
{
    private static Logger           logger            = Logger.getLogger(CDCMetadataFilter.class);

    private String                  schemaNameSuffix;
    private String                  tableNameSuffix;
    private String                  toSingleSchema;
    private long                    sequenceBeginning = 1;

    /**
     * Cache of last sequence numbers in a given change table:</br>
     * "schema.table" => lastSeq
     */
    private Hashtable<String, Long> seqCache;

    /**
     * Name of current replication service's internal tungsten schema.
     */
    private String                  tungstenSchema;

    Database                        conn   = null;

    private String                  user;
    private String                  url;
    private String                  password;

    /**
     * Sets the schemaNameSuffix value. Can be left empty, if change tables are
     * in the same schema as origin tables.
     * 
     * @param schemaNameSuffix The schemaNameSuffix to set.
     */
    public void setSchemaNameSuffix(String schemaNameSuffix)
    {
        this.schemaNameSuffix = schemaNameSuffix;
    }

    /**
     * Sets the tableNameSuffix value. Eg. if tabe name is FOO and suffix
     * is set to _CD, then change rows will be saved in table FOO_CD.
     * 
     * @param tableNameSuffix The tableNameSuffix to set.
     */
    public void setTableNameSuffix(String tableNameSuffix)
    {
        this.tableNameSuffix = tableNameSuffix;
    }
    
    /**
     * It is possibly to have all CDC tables in a single schema.
     * 
     * @param toSingleSchema Schema where all change tables are expected to be.
     */
    public void setToSingleSchema(String toSingleSchema)
    {
        this.toSingleSchema = toSingleSchema;
    }

    /**
     * Which CDC sequence number to begin with, if CDC sequence number cannot be
     * determined (eg. no CDC data yet exists).
     * 
     * @param sequenceBeginning CDC sequence number.
     */
    public void setSequenceBeginning(long sequenceBeginning)
    {
        this.sequenceBeginning = sequenceBeginning;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.filter.Filter#filter(com.continuent.tungsten.replicator.event.ReplDBMSEvent)
     */
    public ReplDBMSEvent filter(ReplDBMSEvent event) throws ReplicatorException
    {
        ArrayList<DBMSData> data = event.getData();
        ArrayList<DBMSData> dataToAdd = new ArrayList<DBMSData>();
        for (Iterator<DBMSData> iterator = data.iterator(); iterator.hasNext();)
        {
            DBMSData dataElem = iterator.next();
            if (dataElem instanceof RowChangeData)
            {
                RowChangeData cdcData = new RowChangeData();
                dataToAdd.add(cdcData);
                RowChangeData rdata = (RowChangeData) dataElem;
                for (Iterator<OneRowChange> iterator2 = rdata.getRowChanges()
                        .iterator(); iterator2.hasNext();)
                {
                    OneRowChange orc = iterator2.next();

                    // Don't add CDC rows for tables from Tungsten schema.
                    if (orc.getSchemaName().compareToIgnoreCase(tungstenSchema) == 0)
                    {
                        if (logger.isDebugEnabled())
                            logger.debug("Ignoring " + tungstenSchema
                                    + " schema");
                        continue;
                    }

                    // Rename schema.
                    String schemaCDC;
                    if (toSingleSchema != null && toSingleSchema.length() > 0)
                        schemaCDC = toSingleSchema;
                    else
                        schemaCDC = orc.getSchemaName();
                    if (schemaNameSuffix != null
                            && schemaNameSuffix.length() > 0)
                        schemaCDC.concat(schemaNameSuffix);
                    // Rename table.
					String tableCDC = orc.getTableName() + tableNameSuffix;

					OneRowChange cdcRowChangeData = new OneRowChange(schemaCDC,
							tableCDC, ActionType.INSERT);
                    cdcData.appendOneRowChange(cdcRowChangeData);

                    ArrayList<ColumnSpec> cdcSpecs = cdcRowChangeData
                            .getColumnSpec();
                    ArrayList<ArrayList<ColumnVal>> cdcValues = cdcRowChangeData
                            .getColumnValues();
                    
                    if (orc.getAction() == ActionType.DELETE)
                    {
                        // For DELETE, get the key values
                        ArrayList<ColumnSpec> colSpecs = orc.getKeySpec();

                        for (ColumnSpec sourceSpec : colSpecs)
                        {
                            ColumnSpec spec = cdcRowChangeData.new ColumnSpec(
                                    sourceSpec);
                            spec.setIndex(sourceSpec.getIndex());
                            cdcSpecs.add(spec);
                        }

                        ArrayList<ArrayList<ColumnVal>> colValues = orc
                                .getKeyValues();

                        for (ArrayList<ColumnVal> values : colValues)
                        {
                            ArrayList<ColumnVal> val = new ArrayList<OneRowChange.ColumnVal>();
                            cdcValues.add(val);

                            // Original values.
                            for (ColumnVal columnVal : values)
                            {
                                val.add(columnVal);
                            }

                            // CDC values.
                            ColumnVal colVal = cdcRowChangeData.new ColumnVal();
                            colVal.setValue(orc.getAction().toString()
                                    .substring(0, 1));
                            val.add(colVal);
                            colVal = cdcRowChangeData.new ColumnVal();
                            colVal.setValue(event.getDBMSEvent()
                                    .getSourceTstamp());
                            val.add(colVal);
                            colVal = cdcRowChangeData.new ColumnVal();
                            colVal.setValue(getNextSeq(schemaCDC, tableCDC));
                            val.add(colVal);
                        }
                    }
                    else
                    {
                        // For INSERTS or UPDATES, get the column values
                        ArrayList<ColumnSpec> colSpecs = orc.getColumnSpec();

                        for (ColumnSpec sourceSpec : colSpecs)
                        {
                            ColumnSpec spec = cdcRowChangeData.new ColumnSpec(
                                    sourceSpec);
                            spec.setIndex(sourceSpec.getIndex());
                            cdcSpecs.add(spec);
                        }

                        ArrayList<ArrayList<ColumnVal>> colValues = orc
                                .getColumnValues();

                        for (ArrayList<ColumnVal> values : colValues)
                        {
                            ArrayList<ColumnVal> val = new ArrayList<OneRowChange.ColumnVal>();
                            cdcValues.add(val);
                            
                            // Original values.
                            for (ColumnVal columnVal : values)
                            {
                                val.add(columnVal);
                            }

                            // CDC values.
                            ColumnVal colVal = cdcRowChangeData.new ColumnVal();
                            colVal.setValue(orc.getAction().toString()
                                    .substring(0, 1));
                            val.add(colVal);
                            colVal = cdcRowChangeData.new ColumnVal();
                            colVal.setValue(event.getDBMSEvent()
                                    .getSourceTstamp());
                            val.add(colVal);
                            colVal = cdcRowChangeData.new ColumnVal();
                            colVal.setValue(getNextSeq(schemaCDC, tableCDC));
                            val.add(colVal);
                        }
                    }
                    
                    // Add CDC columns after original ones.
                    ColumnSpec spec = cdcRowChangeData.new ColumnSpec();
                    spec.setIndex(cdcSpecs.size() + 1);
                    spec.setName("CDC_OP_TYPE");
                    spec.setType(java.sql.Types.VARCHAR);
                    spec.setLength(1); // (I)NSERT, (U)PDATE or (D)ELETE
                    cdcSpecs.add(spec);
                    spec = cdcRowChangeData.new ColumnSpec();
                    spec.setIndex(cdcSpecs.size() + 1);
                    spec.setName("CDC_TIMESTAMP");
                    spec.setType(java.sql.Types.TIMESTAMP);
                    cdcSpecs.add(spec);
                    spec = cdcRowChangeData.new ColumnSpec();
                    spec.setIndex(cdcSpecs.size() + 1);
                    spec.setName("CDC_SEQUENCE_NUMBER");
                    spec.setType(java.sql.Types.BIGINT);
                    cdcSpecs.add(spec);
                }
            }
        }
        event.getData().addAll(dataToAdd);
        return event;
    }
    
    /**
     * Gets next value for the sequence number (primary key) for a particular
     * change table. First, maximum value is retrieved from the database and
     * increased by one. Each subsequent call doesn't use database and retrieves
     * it from the cache instead.
     * 
     * @return Value for the next sequence number for a particular change table.
     */
    private long getNextSeq(String schemaName, String tableName)
            throws ReplicatorException
    {
        String schemaTable = schemaName + "." + tableName;
        
        if (!seqCache.containsKey(schemaTable))
        {
            // Nothing defined yet for this table.
            String query = "SELECT MAX(CDC_SEQUENCE_NUMBER) FROM "
                    + schemaTable;
            Statement st = null;
            ResultSet rs = null;
            try
            {
                st = conn.createStatement();
                rs = st.executeQuery(query);
                if (rs.next())
                {
                    long lastSeq = rs.getLong(1);
                    seqCache.put(schemaTable, lastSeq);
                }
                else
                {
                    if (logger.isDebugEnabled())
                        logger.debug("Max sequence number couldn't be determined, using "
                                + sequenceBeginning
                                + " instead. Query used: "
                                + query);
                    seqCache.put(schemaTable, sequenceBeginning);
                }
            }
            catch (SQLException e)
            {
                throw new ReplicatorException(
                        "Unable to determine next sequence number for CDC table: "
                                + schemaTable + " (note: ignoring schema "
                                + tungstenSchema + ")", e);
            }
            finally
            {
                try
                {
                    // If connection is not autocommit, aggregate MAX(...) might
                    // be taking a lock, so we free it up.
                    if (conn != null)
                        conn.rollback();
                }
                catch (SQLException e)
                {
                    logger.warn("Failed to rollback : " + e);
                }
                if (rs != null)
                {
                    try
                    {
                        rs.close();
                    }
                    catch (SQLException e)
                    {
                    }
                }
                if (st != null)
                {
                    try
                    {
                        st.close();
                    }
                    catch (SQLException e)
                    {
                    }
                }
            }
        }

        long lastSeq = seqCache.get(schemaTable);
        long newSeq = lastSeq + 1;
        seqCache.put(schemaTable, newSeq);
        return newSeq;
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
        seqCache = new Hashtable<String, Long>();
        
        // Load defaults for connection 
        if (url == null)
            url = context.getJdbcUrl("tungsten");
        if (user == null)
            user = context.getJdbcUser();
        if (password == null)
            password = context.getJdbcPassword();
        
        // Connect. 
        try
        {
            conn = DatabaseFactory.createDatabase(url, user, password);
            conn.connect();
        }
        catch (SQLException e)
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
        if (seqCache != null)
        {
            seqCache.clear();
            seqCache = null;
        }
        if (conn != null)
        {
            conn.close();
            conn = null;
        }
    }
}
