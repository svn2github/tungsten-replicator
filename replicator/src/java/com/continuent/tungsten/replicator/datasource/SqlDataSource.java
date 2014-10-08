/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2011-2014 Continuent Inc.
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
 * Contributor(s): 
 */

package com.continuent.tungsten.replicator.datasource;

import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.channel.ShardChannelTable;
import com.continuent.tungsten.replicator.consistency.ConsistencyTable;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.Table;
import com.continuent.tungsten.replicator.heartbeat.HeartbeatTable;
import com.continuent.tungsten.replicator.shard.ShardTable;

/**
 * Implements a data source based on a relational DBMS. This supersedes class
 * com.continuent.tungsten.replicator.thl.CatalogManager.
 */
public class SqlDataSource extends AbstractDataSource
        implements
            UniversalDataSource
{
    private static Logger logger        = Logger.getLogger(SqlDataSource.class);

    // Properties.
    SqlConnectionSpec     connectionSpec;
    private String        initScript    = null;
    boolean               createCatalog = true;
    boolean               logOperations = false;
    boolean               privileged    = false;

    // Catalog tables.
    SqlCommitSeqno        commitSeqno;
    ShardChannelTable     channelTable;

    // SQL connection manager.
    SqlConnectionManager  connectionManager;

    /** Create new instance. */
    public SqlDataSource()
    {
    }

    public SqlConnectionSpec getConnectionSpec()
    {
        return connectionSpec;
    }

    public void setConnectionSpec(SqlConnectionSpec connectionSpec)
    {
        this.connectionSpec = connectionSpec;
    }

    public String getInitScript()
    {
        return initScript;
    }

    public void setInitScript(String initScript)
    {
        this.initScript = initScript;
    }

    public boolean isCreateCatalog()
    {
        return createCatalog;
    }

    /** If this is true, create catalog tables. */
    public void setCreateCatalog(boolean createCatalog)
    {
        this.createCatalog = createCatalog;
    }

    public boolean isLogOperations()
    {
        return logOperations;
    }

    /**
     * If this is true, enable logging of operations on catalog. Otherwise
     * disable logging. This determines whether catalog DDL and updates goes
     * into the MySQL binlog.
     */
    public void setLogOperations(boolean logOperations)
    {
        this.logOperations = logOperations;
    }

    public boolean isPrivileged()
    {
        return privileged;
    }

    /**
     * If this is true, assume account has privileges. This is necessary to
     * control logging.
     */
    public void setPrivileged(boolean privileged)
    {
        this.privileged = privileged;
    }

    /**
     * Instantiate and configure all data source tables.
     */
    @Override
    public void configure() throws ReplicatorException, InterruptedException
    {
        super.configure();
    }

    /**
     * Prepare all data source tables for use.
     */
    @Override
    public void prepare() throws ReplicatorException, InterruptedException
    {
        // Initialize connection manager.
        connectionManager = new SqlConnectionManager();
        connectionManager.setConnectionSpec(connectionSpec);
        connectionManager.setCsvSpec(csv);
        connectionManager.setPrivileged(privileged);
        connectionManager.setLogOperations(logOperations);
        connectionManager.prepare();

        // Prepare commit seqno table. Channels must be set here as they
        // are unsafe to set earlier as the pipeline does not know the value.
        commitSeqno = new SqlCommitSeqno(connectionManager,
                connectionSpec.getSchema(), connectionSpec.getTableType());
        commitSeqno.setChannels(channels);
        commitSeqno.prepare();
    }

    /**
     * {@inheritDoc}
     */
    public void reduce() throws ReplicatorException, InterruptedException
    {
        // If we don't have the commit seqno or connection manager yet there is
        // no work to be done. This can happen if reduce is called before
        // we have completed prepare.
        if (connectionManager == null || commitSeqno == null)
            return;

        // If the data source did not create a catalog, no need to reduce it
        // (issue 1028)
        if (!createCatalog)
            return;

        // Reduce tasks restart points in trep_commit_seqno table if possible.
        // If tasks are reduced, clear the channel table.
        logger.info("Attempting to reduce catalog data: data source=" + name);
        Database conn = null;
        try
        {
            // Connect to DBMS.
            conn = connectionManager.getCatalogConnection();

            // Reduce both task as well as channel assignments.
            boolean reduced = commitSeqno.reduceTasks();
            if (reduced && channelTable != null)
            {
                channelTable.reduceAssignments(conn, channels);
            }
        }
        catch (Exception e)
        {
            logger.warn("Unable to reduce task information", e);
        }
        finally
        {
            if (conn != null)
                connectionManager.releaseCatalogConnection(conn);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.CatalogEntity#release()
     */
    public void release() throws ReplicatorException, InterruptedException
    {
        // Only release if commit seqno is not null.
        if (commitSeqno != null)
        {
            commitSeqno.release();
            commitSeqno = null;
        }

        // Followed by the connection manager.
        if (connectionManager != null)
        {
            connectionManager.release();
            connectionManager = null;
        }
    }

    /**
     * Ensure all tables are ready for use, creating them if necessary.
     */
    @Override
    public void initialize() throws ReplicatorException, InterruptedException
    {
        // Now ensure catalog tables are created and ready to go, but only
        // if that is desired.
        if (createCatalog)
        {
            logger.info("Initializing data source tables: service="
                    + serviceName + " schema=" + connectionSpec.getSchema());
            Database conn = null;
            try
            {
                // Check connectivity to ensure DBMS is available.
                if (this.connectionSpec.supportsCreateDB())
                {
                    // Check connectivity without createDB but tolerate errors.
                    if (checkDBConnectivity(false, true))
                    {
                        logger.info("Confirmed DBMS connection");
                    }
                    else
                    {
                        // Now try to create schema via JDBC URL.
                        logger.info("Attempting to create schema via JDBC");
                        checkDBConnectivity(true, false);
                    }
                }
                else
                {
                    checkDBConnectivity(false, false);
                    logger.info("Confirmed DBMS connection");
                }

                // Connect to DBMS.
                conn = connectionManager.getCatalogConnection();

                // Set default schema if supported. This also creates the schema
                // if possible.
                String schema = connectionSpec.getSchema();
                if (conn.supportsUseDefaultSchema() && schema != null)
                {
                    if (conn.supportsCreateDropSchema())
                    {
                        conn.createSchema(schema);
                    }
                    conn.useDefaultSchema(schema);
                }

                // Create commit seqno table if it does not already exist.
                commitSeqno.initialize();

                // Create consistency table but only if it does not exist.
                Table consistency = ConsistencyTable
                        .getConsistencyTableDefinition(schema);
                if (conn.findTable(consistency.getSchema(),
                        consistency.getName()) == null)
                {
                    conn.createTable(consistency, false,
                            connectionSpec.getTableType());
                }

                // Create heartbeat table if it does not exist.
                HeartbeatTable heartbeatTable = new HeartbeatTable(schema,
                        connectionSpec.getTableType(), serviceName);
                heartbeatTable.initializeHeartbeatTable(conn);

                // Create shard table if it does not exist
                ShardTable shardTable = new ShardTable(schema,
                        connectionSpec.getTableType());
                shardTable.initializeShardTable(conn);

                // Create channel table.
                channelTable = new ShardChannelTable(schema,
                        connectionSpec.getTableType());
                channelTable.initializeShardTable(conn, this.channels);
            }
            catch (SQLException e)
            {
                throw new ReplicatorException(
                        "Unable to create catalog tables", e);
            }
            finally
            {
                if (conn != null)
                {
                    connectionManager.releaseCatalogConnection(conn);
                }
            }
        }
    }

    /**
     * Checks ability to connect to DBMS using JDBC.
     * 
     * @param createDB If true createDB via JDBC URL
     * @param ignoreError If true, ignore JDBC error
     * @return True if able to connect successfully, otherwise false
     */
    private boolean checkDBConnectivity(boolean createDB, boolean ignoreError)
            throws ReplicatorException
    {
        Database conn = null;
        try
        {
            // Try to connect to DBMS, returning true if successful.
            conn = connectionManager.getRawConnection(createDB);
            conn.connect();
            return true;
        }
        catch (SQLException e)
        {
            if (!ignoreError)
            {
                throw new ReplicatorException("Unable to connect to DBMS: url="
                        + connectionSpec.createUrl(createDB));
            }
        }
        finally
        {
            if (conn != null)
            {
                connectionManager.releaseConnection(conn);
            }
        }

        // Connection test failed if we get here.
        return false;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.CatalogEntity#clear()
     */
    @Override
    public boolean clear() throws ReplicatorException, InterruptedException
    {
        // If the data source did not create a catalog, no need to clear it
        // (issue 1028)
        if (!createCatalog)
            return true;

        // See if we can connect.
        if (!checkDBConnectivity(false, true))
        {
            // This is all we can do.
            logger.info("Unable to connect to data source; cannot delete catalog data: name="
                    + name);
            return true;
        }
        else
        {
            String schema = connectionSpec.getSchema();
            logger.info("Clearing catalog data for data source: name=" + name
                    + " schema=" + schema);
            Database conn = null;
            try
            {
                // Drop tables.
                commitSeqno.clear();
                conn = connectionManager.getRawConnection(false);
                conn.connect();
                conn.dropTable(new Table(schema, ConsistencyTable.TABLE_NAME));
                conn.dropTable(new Table(schema, ShardChannelTable.TABLE_NAME));
                conn.dropTable(new Table(schema, ShardTable.TABLE_NAME));
                conn.dropTable(new Table(schema, HeartbeatTable.TABLE_NAME));
                return true;
            }
            catch (SQLException e)
            {
                logger.warn(
                        "Unable to delete data source catalog data: name="
                                + name + " schema=" + schema + " url="
                                + connectionSpec.createUrl(false), e);
                return false;
            }
            finally
            {
                if (conn != null)
                {
                    connectionManager.releaseConnection(conn);
                }
            }
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.UniversalDataSource#getCommitSeqno()
     */
    @Override
    public CommitSeqno getCommitSeqno()
    {
        return commitSeqno;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.UniversalDataSource#getConnection()
     */
    public Database getConnection() throws ReplicatorException
    {
        return connectionManager.getWrappedConnection();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.UniversalDataSource#releaseConnection(com.continuent.tungsten.replicator.datasource.UniversalConnection)
     */
    public void releaseConnection(UniversalConnection conn)
    {
        connectionManager.releaseConnection((Database) conn);
    }
}