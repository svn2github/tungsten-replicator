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
import com.continuent.tungsten.replicator.database.Database;

/**
 * Implements a data source based on a relational DBMS. This supersedes class
 * com.continuent.tungsten.replicator.thl.CatalogManager.
 */
public class SqlDataSource extends AbstractDataSource
        implements
            UniversalDataSource
{
    private static Logger logger = Logger.getLogger(SqlDataSource.class);

    // Properties.
    SqlConnectionSpec     connectionSpec;

    // Catalog tables.
    SqlCommitSeqno        commitSeqno;

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

    /**
     * Instantiate and configure all data source tables.
     */
    @Override
    public void configure() throws ReplicatorException,
            InterruptedException
    {
        super.configure();

        // Configure connection manager.
        connectionManager = new SqlConnectionManager();
        connectionManager.setConnectionSpec(connectionSpec);

        // Configure tables.
        commitSeqno = new SqlCommitSeqno(connectionManager,
                connectionSpec.getSchema(), connectionSpec.getTableType());
        commitSeqno.setChannels(channels);
    }

    /**
     * Prepare all data source tables for use.
     */
    @Override
    public void prepare() throws ReplicatorException, InterruptedException
    {
        // Create an initial connection to ensure we can reach the DBMS.
        Database conn = null;
        try
        {
            conn = connectionManager.getWrappedConnection(true);
            conn.connect();
        }
        catch (SQLException e)
        {
            throw new ReplicatorException(
                    "Unable to connect to data source: url="
                            + this.connectionSpec.createUrl(true));
        }
        finally
        {
            if (conn != null)
            {
                connectionManager.releaseWrappedConnection(conn);
                conn = null;
            }
        }

        // First of course prepare the usual manager.
        connectionManager.prepare();

        // Now prepare all tables.
        commitSeqno.prepare();
    }

    /**
     * Release all data source tables.
     */
    @Override
    public void release() throws ReplicatorException, InterruptedException
    {
        // Release tables first...
        if (commitSeqno != null)
        {
            commitSeqno.reduceTasks();
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
        logger.info("Initializing data source tables: service=" + serviceName
                + " schema=" + connectionSpec.getSchema());
        commitSeqno.initialize();
    }

    @Override
    public void clear() throws ReplicatorException, InterruptedException
    {
        commitSeqno.clear();
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
    public UniversalConnection getConnection() throws ReplicatorException
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
        connectionManager.releaseWrappedConnection((Database) conn);
    }
}