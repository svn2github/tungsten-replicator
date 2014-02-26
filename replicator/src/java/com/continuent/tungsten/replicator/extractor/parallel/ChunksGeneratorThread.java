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
 * Initial developer(s): Stephane Giron
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.extractor.parallel;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.DatabaseFactory;
import com.continuent.tungsten.replicator.database.Table;
import com.continuent.tungsten.replicator.extractor.parallel.ChunkDefinitions.ChunkRequest;

/**
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class ChunksGeneratorThread extends Thread
{

    private class MinMax
    {

        private long   count;

        private Number min;

        private Number max;

        public MinMax(Number min, Number max, long count)
        {
            this.min = min;
            this.max = max;
            this.count = count;
        }

        /**
         * Returns the count value.
         * 
         * @return Returns the count.
         */
        protected long getCount()
        {
            return count;
        }

        /**
         * Returns the min value.
         * 
         * @return Returns the min.
         */
        protected Number getMin()
        {
            return min;
        }

        /**
         * Returns the max value.
         * 
         * @return Returns the max.
         */
        protected Number getMax()
        {
            return max;
        }

        protected boolean isDecimal()
        {
            return !(this.min instanceof Long);
        }

    }

    private static Logger        logger    = Logger.getLogger(ChunksGeneratorThread.class);
    private Database             connection;
    private String               user;
    private String               url;
    private String               password;
    private BlockingQueue<Chunk> chunks;
    private String               chunkDefFile;
    private ChunkDefinitions     chunkDefinition;
    private int                  extractChannels;
    private long                 chunkSize = 1000;
    private String               eventId   = null;

    /**
     * Creates a new <code>TableGeneratorThread</code> object
     * 
     * @param user
     * @param url
     * @param password
     * @param extractChannels
     * @param chunks
     * @param chunkDefinitionFile
     * @param chunkSize
     */
    public ChunksGeneratorThread(String user, String url, String password,
            int extractChannels, BlockingQueue<Chunk> chunks,
            String chunkDefinitionFile, long chunkSize)
    {
        this.setName("ChunkGeneratorThread");
        this.user = user;
        this.url = url;
        this.password = password;
        this.chunks = chunks;
        this.chunkDefFile = chunkDefinitionFile;
        this.extractChannels = extractChannels;
        if (chunkSize > 0)
            this.chunkSize = chunkSize;
    }

    public void run()
    {
        try
        {
            runTask();
        }
        catch (Exception e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * TODO: runTask definition.
     */
    private void runTask()
    {
        connection = null;
        try
        {
            connection = DatabaseFactory.createDatabase(url, user, password);
        }
        catch (SQLException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        try
        {
            connection.connect();
        }
        catch (SQLException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        // Check whether we have to use a chunk definition file
        if (chunkDefFile != null)
        {
            logger.info("Using definition from file " + chunkDefFile);
            chunkDefinition = new ChunkDefinitions(chunkDefFile);
            try
            {
                chunkDefinition.parseFile();
            }
            catch (IOException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            catch (ReplicatorException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            LinkedList<ChunkRequest> chunksDefinitions = chunkDefinition
                    .getChunksDefinitions();
            for (ChunkRequest chunkRequest : chunksDefinitions)
            {
                if (chunkRequest.getTable() != null)
                {
                    try
                    {
                        Table table = connection.findTable(
                                chunkRequest.getSchema(),
                                chunkRequest.getTable());

                        if (table != null)
                            generateChunksForTable(table,
                                    chunkRequest.getChunkSize(),
                                    chunkRequest.getColumns());
                        else
                            logger.warn("Failed while processing table "
                                    + chunkRequest.getSchema() + "."
                                    + chunkRequest.getTable()
                                    + " : table not found.");
                    }
                    catch (SQLException e)
                    {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    catch (ReplicatorException e)
                    {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    catch (InterruptedException e)
                    {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
                else if (chunkRequest.getSchema() != null)
                {
                    generateChunksForSchema(chunkRequest.getSchema());
                }
            }
        }
        else
        {
            try
            {
                DatabaseMetaData databaseMetaData = connection
                        .getDatabaseMetaData();
                ResultSet schemasRs = databaseMetaData.getSchemas();
                while (schemasRs.next())
                {
                    String schemaName = schemasRs.getString("TABLE_SCHEM");
                    // TODO: System schemas could be needed -> this needs a
                    // setting
                    if (!connection.isSystemSchema(schemaName))
                    {
                        generateChunksForSchema(schemaName);
                    }
                }
                schemasRs.close();
            }
            catch (SQLException e)
            {
                logger.error(e);
            }
            catch (Exception e)
            {
                logger.error(e);
            }
        }

        // Stop threads
        for (int i = 0; i < extractChannels; i++)
        {
            logger.info("Posting job complete request " + i);
            try
            {
                chunks.put(new NumericChunk());
            }
            catch (InterruptedException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        if (logger.isDebugEnabled())
            logger.debug(this.getName() + " done.");
    }

    /**
     * TODO: generateChunksForSchema definition.
     * 
     * @param schemaName
     */
    private void generateChunksForSchema(String schemaName)
    {
        try
        {
            if (logger.isDebugEnabled())
                logger.debug("Getting list of tables from " + schemaName);

            ArrayList<Table> tablesFromSchema = connection.getTables(
                    schemaName, true);
            if (logger.isDebugEnabled())
                logger.debug("Tables : " + tablesFromSchema);
            if (tablesFromSchema != null && tablesFromSchema.size() > 0)
            {
                for (Table table : tablesFromSchema)
                {
                    generateChunksForTable(table, -1, null);
                }

            }
        }
        catch (Exception e)
        {
            // TODO: handle exception
            e.printStackTrace();

        }
    }

    /**
     * TODO: generateChunksForTable definition.
     * 
     * @param table
     * @param strings
     * @param l
     * @throws ReplicatorException
     * @throws InterruptedException
     */
    private void generateChunksForTable(Table table, long tableChunkSize,
            String[] columns) throws ReplicatorException, InterruptedException
    {
        long chunkSize;

        Integer pkType = getPKType(table);

        if (pkType != null && tableChunkSize == 0)
        {
            // No chunks for this table (all table at once)
            if (pkType == Types.NUMERIC)
                chunks.put(new NumericChunk(table, columns));
            else
                chunks.put(new StringChunk(table, null, null));

            return;
        }
        else if (tableChunkSize < 0)
        {
            // Use default chunk size
            chunkSize = this.chunkSize;
        }
        else
        {
            chunkSize = tableChunkSize;
        }

        // if (logger.isDebugEnabled())
        logger.info("Processing table " + table.getSchema() + "."
                + table.getName());

        if (pkType == null)
            chunkLimit(table);
        else if (pkType == Types.NUMERIC)
            chunkNumericPK(table, columns, chunkSize);
        else if (pkType == Types.VARCHAR)
            chunkVarcharPK(table);
    }

    /**
     * TODO: chunkNumericPK definition.
     * 
     * @param table
     * @param columns
     * @param chunkSize
     * @throws ReplicatorException
     * @throws InterruptedException
     */
    private void chunkNumericPK(Table table, String[] columns, long chunkSize)
            throws ReplicatorException, InterruptedException
    {
        // Retrieve PK range
        MinMax minmax = retrieveMinMaxCountPK(connection, table);

        if (minmax != null)
        {
            // if (logger.isDebugEnabled())
            logger.info("Min = " + minmax.getMin() + " -- Max = "
                    + minmax.getMax() + " -- Count = " + minmax.getCount());

            if (minmax.getCount() <= chunkSize)
                // Get the whole table at once
                chunks.put(new NumericChunk(table, columns));
            else
            {
                // Share the joy among threads,
                // if primary key is evenly distributed
                if (!minmax.isDecimal())
                {
                    long gap = (Long) minmax.getMax() - (Long) minmax.getMin();
                    long blockSize = chunkSize * gap / minmax.getCount();

                    long nbBlocks = gap / blockSize;
                    if (gap % blockSize > 0)
                        nbBlocks++;

                    long start = (Long) minmax.getMin() - 1;
                    long end;
                    do
                    {
                        end = start + blockSize;
                        if (end > (Long) minmax.getMax())
                            end = (Long) minmax.getMax();
                        NumericChunk e = new NumericChunk(table, start, end,
                                columns, nbBlocks);
                        chunks.put(e);
                        start = end;
                    }
                    while (start < (Long) minmax.getMax());

                }
                else
                {
                    BigInteger gap = (((BigDecimal) minmax.getMax())
                            .subtract((BigDecimal) minmax.getMin())).setScale(
                            0, RoundingMode.CEILING).toBigInteger();

                    BigInteger blockSize = gap.multiply(
                            BigInteger.valueOf(chunkSize)).divide(
                            BigInteger.valueOf(minmax.getCount()));
                    long nbBlocks = gap.divide(blockSize).longValue();
                    if (!gap.remainder(blockSize).equals(BigInteger.ZERO))
                        nbBlocks++;

                    BigDecimal start = ((BigDecimal) minmax.getMin()).setScale(
                            0, RoundingMode.FLOOR);
                    BigDecimal end;
                    do
                    {
                        end = start.add(new BigDecimal(blockSize));
                        if (end.compareTo((BigDecimal) minmax.getMax()) == 1)
                            end = (BigDecimal) minmax.getMax();

                        end = end.setScale(0, RoundingMode.CEILING);

                        NumericChunk e = new NumericChunk(table,
                                start.toBigInteger(), end.toBigInteger(),
                                columns, nbBlocks);
                        chunks.put(e);
                        start = end;
                    }
                    while (start.compareTo(((BigDecimal) minmax.getMax())
                            .setScale(0, RoundingMode.CEILING)) == -1);

                }

            }
        }
        else
        {
            // table is empty or does not have a
            // good candidate as a PK for chunking.
            // Fall back to limit method
            chunks.put(new NumericChunk(table, columns));
        }
    }

    /**
     * Retrieve maximum or minimum value of a table's primary key. Table must
     * have a single-column numeric key for this to work correctly.
     */
    private MinMax retrieveMinMaxCountPK(Database conn, Table table)
            throws ReplicatorException
    {

        String pkName = table.getPrimaryKey().getColumns().get(0).getName();
        String sql = String.format(
                "SELECT MIN(%s),MAX(%s), COUNT(%s) FROM %s",
                pkName,
                pkName,
                pkName,
                conn.getDatabaseObjectName(table.getSchema()) + '.'
                        + conn.getDatabaseObjectName(table.getName()));
        if (eventId != null)
            sql += " AS OF SCN " + eventId;

        Statement st = null;
        ResultSet rs = null;
        try
        {
            st = conn.createStatement();
            rs = st.executeQuery(sql);
            if (rs.next())
            {
                Object min = rs.getObject(1);
                Object max = rs.getObject(2);
                if (min instanceof Long && max instanceof Long)
                {
                    return new MinMax(((Long) min), ((Long) max), rs.getLong(3));

                }
                else if (min instanceof BigDecimal && max instanceof BigDecimal)
                {
                    return new MinMax(((BigDecimal) min), ((BigDecimal) max),
                            rs.getLong(3));

                }
                return null;
            }
        }
        catch (Exception e)
        {
            logger.warn("Failed to retrieve min, max and count values for PK "
                    + pkName + " in table "
                    + conn.getDatabaseObjectName(table.getSchema()) + '.'
                    + conn.getDatabaseObjectName(table.getName()));
        }
        finally
        {
            if (rs != null)
            {
                try
                {
                    rs.close();
                }
                catch (SQLException e)
                {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
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
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
        return null;
    }

    /**
     * TODO: getPKType definition.
     * 
     * @param table
     */
    private Integer getPKType(Table table)
    {
        if (table.getPrimaryKey() == null)
        {
            logger.warn(table.getName() + " has no PK");
            return null;
        }
        else if (table.getPrimaryKey().getColumns().size() != 1)
        {
            logger.warn(table.getName() + " - PK is not a single-column one "
                    + table.getPrimaryKey().getColumns());
            return null;
        }
        else
        {
            // Check whether primary key is NUMBER based
            int type = table.getPrimaryKey().getColumns().get(0).getType();

            if (type == Types.VARCHAR
                    || (type == Types.OTHER && table.getPrimaryKey()
                            .getColumns().get(0).getTypeDescription()
                            .contains("VARCHAR")))
            {
                return Types.VARCHAR;
            }
            else if ((type == Types.BIGINT || type == Types.INTEGER
                    || type == Types.SMALLINT || type == Types.DECIMAL))
            {
                return Types.NUMERIC;
            }
            else
            {
                logger.warn(table.getName()
                        + " - PK is not a supported chunking datatype ");
                return null;
            }
        }
    }

    private void chunkVarcharPK(Table table) throws InterruptedException
    {
        String pkName = table.getPrimaryKey().getColumns().get(0).getName();
        String fqnTable = connection.getDatabaseObjectName(table.getSchema())
                + '.' + connection.getDatabaseObjectName(table.getName());
        // Get Count
        String sql = String.format("SELECT COUNT(%s) as cnt FROM %s", pkName,
                fqnTable);
        if (eventId != null)
            sql += " AS OF SCN " + eventId;

        // if count <= Chunk size, we are done
        long count = 0;
        Statement st = null;
        ResultSet rs = null;
        try
        {
            st = connection.createStatement();
            rs = st.executeQuery(sql);
            if (rs.next())
            {
                count = rs.getLong("cnt");
            }
        }
        catch (Exception e)
        {
            logger.warn("Failed to retrieve count value for PK " + pkName
                    + " in table " + fqnTable, e);
        }
        finally
        {
            if (rs != null)
            {
                try
                {
                    rs.close();
                }
                catch (SQLException e)
                {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
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
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }

        if (count == 0)
            return;

        if (count <= chunkSize)
        {
            chunks.put(new StringChunk(table, null, null));
            return;
        }

        // Else (count > CHUNK_SIZE) : chunk again in smaller parts
        long nbBlocks = count / chunkSize;

        if (count % chunkSize > 0)
            nbBlocks++;

        long blockSize = count / nbBlocks;

        PreparedStatement pstmt = null;

        StringBuffer sqlBuf = new StringBuffer("SELECT MIN(");
        sqlBuf.append(pkName);
        sqlBuf.append(") as min, MAX(");
        sqlBuf.append(pkName);
        sqlBuf.append(") as max, COUNT(");
        sqlBuf.append(pkName);
        sqlBuf.append(") as cnt FROM ( select sub.*, ROWNUM rnum from ( SELECT ");
        sqlBuf.append(pkName);
        sqlBuf.append(" FROM ");
        sqlBuf.append(fqnTable);
        sqlBuf.append(" ORDER BY ");
        sqlBuf.append(pkName);

        if (eventId != null)
        {
            sqlBuf.append(" AS OF SCN ");
            sqlBuf.append(eventId);
        }

        sqlBuf.append(") sub where ROWNUM <= ? ) where rnum >= ?");

        sql = sqlBuf.toString();

        try
        {
            pstmt = connection.prepareStatement(sql);
        }
        catch (SQLException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        try
        {
            for (long i = 0; i < count; i += blockSize + 1)
            {
                try
                {
                    pstmt.setLong(1, i + blockSize);
                    pstmt.setLong(2, i);
                    rs = pstmt.executeQuery();

                    if (rs.next())
                        chunks.put(new StringChunk(table, rs.getString("min"),
                                rs.getString("max"), nbBlocks));

                }
                catch (SQLException e)
                {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                finally
                {
                    try
                    {
                        rs.close();
                    }
                    catch (SQLException e)
                    {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }
        }
        finally
        {
            try
            {
                pstmt.close();
            }
            catch (SQLException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    private void chunkLimit(Table table) throws InterruptedException
    {
        // Table does not have a primary key. Let's chunk using limit.
        String fqnTable = connection.getDatabaseObjectName(table.getSchema())
                + '.' + connection.getDatabaseObjectName(table.getName());

        // Get Count
        String sql = String.format("SELECT COUNT(*) as cnt FROM %s", fqnTable);
        if (eventId != null)
            sql += " AS OF SCN " + eventId;

        long count = 0;
        Statement st = null;
        ResultSet rs = null;
        try
        {
            st = connection.createStatement();
            rs = st.executeQuery(sql);
            if (rs.next())
            {
                count = rs.getLong("cnt");
            }
        }
        catch (Exception e)
        {
            logger.warn("Failed to retrieve row count values for table "
                    + fqnTable, e);
        }
        finally
        {
            if (rs != null)
            {
                try
                {
                    rs.close();
                }
                catch (SQLException e)
                {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
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
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
        if (count == 0)
            return;

        if (count <= chunkSize)
        {
            chunks.put(new LimitChunk(table));
            return;
        }

        // Else (count > CHUNK_SIZE) : chunk again in smaller parts
        long nbBlocks = count / chunkSize;

        if (count % chunkSize > 0)
            nbBlocks++;

        long blockSize = (long) Math.ceil((double) count / (double) nbBlocks);

        for (long i = 0; i < count; i += blockSize + 1)
        {
            chunks.put(new LimitChunk(table, i, i + blockSize, nbBlocks));
        }

    }

    public void setEventId(String eventId)
    {
        this.eventId = eventId;
    }
}
