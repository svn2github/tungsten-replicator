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
 * Initial developer(s): Robert Hodges
 * Contributor(s): 
 */

package com.continuent.tungsten.replicator.datasource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.DatabaseFactory;

/**
 * Encapsulates connection creation and release for SQL data sources and adds
 * convenience method for management of various JDBC objects.
 */
public class SqlConnectionManager
{
    // Properties.
    private SqlConnectionSpec connectionSpec;

    /**
     * Creates a new instance.
     */
    public SqlConnectionManager()
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
     * Prepares connection pool for use. This must be called before requesting
     * any connections.
     */
    public void prepare() throws ReplicatorException
    {
    }

    /**
     * Frees all resources. This must be called after use to avoid resource
     * leaks.
     */
    public void release() throws ReplicatorException
    {
    }

    /**
     * Returns a JDBC connection.
     */
    public Database getWrappedConnection() throws ReplicatorException
    {
        return this.getWrappedConnection(false);
    }

    /**
     * Gets a JDBC connection wrapped in a Database instance.
     * 
     * @param createDB If true and the JDBC driver supports such an option, add
     *            URL option to create schema
     */
    public Database getWrappedConnection(boolean createDB)
            throws ReplicatorException
    {
        String url = connectionSpec.createUrl(createDB);
        String user = connectionSpec.getUser();
        String password = connectionSpec.getPassword();
        boolean privilegedSlaveUpdate = connectionSpec
                .isPrivilegedSlaveUpdate();
        boolean logSlaveUpdates = connectionSpec.isLogSlaveUpdates();

        try
        {

            Database conn = DatabaseFactory.createDatabase(url, user, password,
                    privilegedSlaveUpdate);
            conn.connect();
            if (!logSlaveUpdates && privilegedSlaveUpdate)
            {
                // If we are a slave with super power and we do not want to
                // log updates, turn off logging.
                if (conn.supportsControlSessionLevelLogging())
                {
                    conn.controlSessionLevelLogging(true);
                }
            }
            return conn;
        }
        catch (SQLException e)
        {
            throw new ReplicatorException("Unable to connect to DBMS: url="
                    + url, e);
        }
    }

    /**
     * Releases a wrapped connection.
     */
    public void releaseWrappedConnection(Database conn)
    {
        if (conn != null)
            conn.close();
    }

    /**
     * Convenience function to close a possibly null ResultSet suppressing any
     * exceptions.
     */
    public void close(ResultSet rs)
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
    }

    /**
     * Convenience function to close a possibly null Statement suppressing any
     * exceptions.
     */
    public void close(Statement s)
    {
        if (s != null)
        {
            try
            {
                s.close();
            }
            catch (SQLException e)
            {
            }
        }
    }
}