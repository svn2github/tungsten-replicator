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
 * Initial developer(s): Robert Hodges
 * Contributor(s): Linas Virbalas, Stephane Giron
 */

package com.continuent.tungsten.replicator.scripting;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.database.Database;

/**
 * Provides a simple wrapper for JDBC connections that is suitable for exposure
 * in scripted environments. This class may be extended to allow additional
 * methods for specific DBMS types.
 */
public class SqlWrapper
{
    private static Logger    logger = Logger.getLogger(SqlWrapper.class);

    // DBMS connection and statement.
    protected final Database connection;
    protected Statement      statement;

    /** Creates a new instance. */
    public SqlWrapper(Database connection) throws SQLException
    {
        this.connection = connection;
        statement = connection.createStatement();
    }

    /**
     * Executes a SQL statement.
     */
    public int execute(String sql) throws SQLException
    {
        if (logger.isDebugEnabled())
            logger.debug("Executing SQL: " + sql);
        return statement.executeUpdate(sql);
    }

    /**
     * Does a COUNT on a given table.
     * 
     * @param tableName Fully qualified table name (with schema name).
     * @return Row count in the table.
     */
    public int retrieveRowCount(String tableName) throws SQLException
    {
        ResultSet rs = null;
        int rowCount = -1;
        try
        {
            rs = statement.executeQuery("SELECT COUNT(*) FROM " + tableName);
            rs.next();
            rowCount = rs.getInt(1);
        }
        finally
        {
            if (rs != null)
            {
                rs.close();
            }
        }
        return rowCount;
    }

    /**
     * Begins a DBMS transaction.
     */
    public void begin() throws SQLException
    {
        if (logger.isDebugEnabled())
            logger.debug("Beginning transaction");
        connection.setAutoCommit(false);
    }

    /**
     * Commits a DBMS transaction.
     */
    public void commit() throws SQLException
    {
        if (logger.isDebugEnabled())
            logger.debug("Committing transaction");
        connection.commit();
    }

    /**
     * Rolls back a DBMS transaction.
     */
    public void rollback() throws SQLException
    {
        if (logger.isDebugEnabled())
            logger.debug("Rolling back transaction");
        connection.rollback();
    }

    /**
     * Releases the statement.
     */
    public void close()
    {
        // Release statement.
        if (statement != null)
        {
            try
            {
                statement.close();
            }
            catch (SQLException e)
            {
                if (logger.isDebugEnabled())
                    logger.debug("Unable to close statement", e);
            }
        }
    }
}