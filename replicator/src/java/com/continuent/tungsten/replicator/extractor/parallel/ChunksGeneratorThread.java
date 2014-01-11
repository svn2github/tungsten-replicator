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
import java.sql.DatabaseMetaData;
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
    // this is the default value
    // TODO : add a setting to override it
    private static final long CHUNK_SIZE = 1000;

    private class MinMax
    {

        private long min;

        /**
         * Returns the min value.
         * 
         * @return Returns the min.
         */
        protected long getMin()
        {
            return min;
        }

        /**
         * Returns the max value.
         * 
         * @return Returns the max.
         */
        protected long getMax()
        {
            return max;
        }

        private long max;
        private long count;

        /**
         * Returns the count value.
         * 
         * @return Returns the count.
         */
        protected long getCount()
        {
            return count;
        }

        public MinMax(long min, long max, long count)
        {
            this.min = min;
            this.max = max;
            this.count = count;
        }

    }

    private static Logger        logger = Logger.getLogger(ChunksGeneratorThread.class);
    private Database             connection;
    private String               user;
    private String               url;
    private String               password;
    private BlockingQueue<Chunk> chunks;
    private String               chunkDefFile;
    private ChunkDefinitions     chunkDefinition;
    private int                  extractChannels;

    /**
     * Creates a new <code>TableGeneratorThread</code> object
     * 
     * @param user
     * @param url
     * @param password
     * @param extractChannels
     * @param chunks
     * @param chunkDefinitionFile
     */
    public ChunksGeneratorThread(String user, String url, String password,
            int extractChannels, BlockingQueue<Chunk> chunks,
            String chunkDefinitionFile)
    {
        this.setName("ChunkGeneratorThread");
        this.user = user;
        this.url = url;
        this.password = password;
        this.chunks = chunks;
        this.chunkDefFile = chunkDefinitionFile;
        this.extractChannels = extractChannels;
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
        }

        try
        {
            connection.connect();
        }
        catch (SQLException e)
        {
            // throw new ReplicatorException("Unable to connect to Oracle", e);
        }

        // Check whether we have to use a chunk definition file
        if (chunkDefFile != null)
        {
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
                        generateChunksForTable(table,
                                chunkRequest.getChunkSize(),
                                chunkRequest.getColumns());
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
                chunks.put(new Chunk());
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
        if (tableChunkSize < 0)
        {
            // Use default chunk size
            chunkSize = CHUNK_SIZE;
        }
        else if (tableChunkSize == 0)
        {
            // No chunks for this table (all table at once)
            chunks.put(new Chunk(table, columns));
            return;
        }
        else
        {
            chunkSize = tableChunkSize;
        }

        // if (logger.isDebugEnabled())
        logger.info("Processing table " + table.getSchema() + "."
                + table.getName());

        // Retrieve PK range
        MinMax minmax = retrieveMaxMinCountPK(connection, table);

        if (minmax != null)
        {
            // if (logger.isDebugEnabled())
            logger.info("Min = " + minmax.getMin() + " -- Max = "
                    + minmax.getMax() + " -- Count = " + minmax.getCount());

            if (minmax.getCount() <= chunkSize)
                // Get the whole table at once
                chunks.put(new Chunk(table, columns));
            else
            {
                // Share the joy among threads,
                // if primary key is evenly
                // distributed
                long blocksize = minmax.getCount() % chunkSize;

                long start = minmax.getMin();
                long end;
                do
                {
                    end = start + blocksize;
                    if (end > minmax.getMax())
                        end = minmax.getMax();
                    Chunk e = new Chunk(table, start, end, columns);
                    chunks.put(e);
                    start = end + 1;
                }
                while (start < minmax.getMax());
            }
        }
        else
        {
            // table is empty or does not have a
            // good candidate as a PK for chunking.
            // Fall back to limit method
            chunks.put(new Chunk(table, columns));
        }
    }

    /**
     * Retrieve maximum or minimum value of a table's primary key. Table must
     * have a single-column numeric key for this to work correctly.
     */
    private MinMax retrieveMaxMinCountPK(Database conn, Table table)
            throws ReplicatorException
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
            // Check whether primary key is INTEGER based
            int type = table.getPrimaryKey().getColumns().get(0).getType();

            if (!(type == Types.BIGINT || type == Types.INTEGER
                    || type == Types.SMALLINT || type == Types.DECIMAL))
                return null;
        }

        String pkName = table.getPrimaryKey().getColumns().get(0).getName();
        String query = String.format(
                "SELECT MIN(%s),MAX(%s), COUNT(%s) FROM %s", pkName, pkName,
                pkName, conn.getDatabaseObjectName(table.getSchema()) + '.'
                        + conn.getDatabaseObjectName(table.getName()));

        Statement st = null;
        ResultSet rs = null;
        try
        {
            st = conn.createStatement();
            rs = st.executeQuery(query);
            if (rs.next())
            {
                Object min = rs.getObject(1);
                Object max = rs.getObject(2);
                if (min instanceof Long && max instanceof Long)
                {
                    return new MinMax(((Long) min).longValue(),
                            ((Long) max).longValue(), rs.getLong(3));

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
        return null;
    }
}
