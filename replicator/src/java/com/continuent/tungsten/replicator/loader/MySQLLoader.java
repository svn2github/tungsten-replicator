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
 * Initial developer(s): Jeff Mace
 */

package com.continuent.tungsten.replicator.loader;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.plugin.PluginContext;

public class MySQLLoader extends JdbcLoader
{
    private static Logger logger = Logger.getLogger(MySQLLoader.class);

    /**
     * Creates a new <code>MySQLLoader</code> object
     */
    public MySQLLoader()
    {
        super();

        setLockTables(true);
    }

    /**
     * Build a MySQL JDBC connection string {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.loader.JdbcLoader#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        if (url == null)
        {
            StringBuffer sb = new StringBuffer();
            sb.append("jdbc:mysql:thin://");
            sb.append(uri.getHost());
            if (uri.getPort() > 0)
            {
                sb.append(":");
                sb.append(uri.getPort());
            }
            sb.append("/");
            if (uri.getPath() != null)
                sb.append(uri.getPath());
            if (uri.getQuery() != null)
                sb.append(uri.getQuery());

            url = sb.toString();
        }
        else if (logger.isDebugEnabled())
            logger.debug("Property url already set; ignoring host and port properties");

        super.configure(context);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.loader.JdbcLoader#lockTables()
     */
    public void lockTables() throws SQLException
    {
        logger.info("Run FLUSH TABLES to lock out changes");
        statement.execute("FLUSH NO_WRITE_TO_BINLOG TABLES WITH READ LOCK");
    }

    /**
     * {@inheritDoc}
     * 
     * @throws SQLException
     * @see com.continuent.tungsten.replicator.loader.JdbcLoader#buildCreateSchemaStatement()
     */
    protected String buildCreateSchemaStatement() throws ReplicatorException
    {
        ResultSet createSchemaResult = null;

        try
        {
            createSchemaResult = statement
                    .executeQuery("SHOW CREATE DATABASE IF NOT EXISTS `"
                            + importTables.getString("TABLE_SCHEM") + "`");
            if (createSchemaResult.first() != true)
            {
                throw new ReplicatorException(
                        "Unable to extract the CREATE DATABASE statement for "
                                + importTables.getString("TABLE_SCHEM"));
            }

            return createSchemaResult.getString("Create Database");
        }
        catch (SQLException e)
        {
            throw new ReplicatorException(e);
        }
        finally
        {
            if (createSchemaResult != null)
            {
                try
                {
                    createSchemaResult.close();
                }
                catch (SQLException e)
                {
                    throw new ReplicatorException(e);
                }
            }
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @throws SQLException
     * @see com.continuent.tungsten.replicator.loader.JdbcLoader#buildCreateTableStatement()
     */
    protected String buildCreateTableStatement() throws ReplicatorException
    {
        ResultSet createTableResult = null;

        try
        {
            createTableResult = statement.executeQuery("SHOW CREATE TABLE `"
                    + importTables.getString("TABLE_SCHEM") + "`.`"
                    + importTables.getString("TABLE_NAME") + "`");
            if (createTableResult.first() != true)
            {
                throw new ReplicatorException(
                        "Unable to extract the CREATE TABLE statement for "
                                + importTables.getString("TABLE_SCHEM") + "."
                                + importTables.getString("TABLE_NAME"));
            }

            return createTableResult.getString("Create Table");
        }
        catch (SQLException e)
        {
            throw new ReplicatorException(e);
        }
        finally
        {
            if (createTableResult != null)
            {
                try
                {
                    createTableResult.close();
                }
                catch (SQLException e)
                {
                    throw new ReplicatorException(e);
                }
            }
        }
    }

    /**
     * Use the output of SHOW MASTER STATUS to get the extractor Event ID
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.RawExtractor#getCurrentResourceEventId()
     */
    @Override
    public String getCurrentResourceEventId() throws ReplicatorException,
            InterruptedException
    {
        ResultSet masterStatus = null;
        String currentResourceEventId = super.getCurrentResourceEventId();

        if (currentResourceEventId == null)
        {
            try
            {
                masterStatus = statement.executeQuery("SHOW MASTER STATUS");

                if (masterStatus.next())
                {
                    String fileName = masterStatus.getString("File");
                    int dotIndex = fileName.indexOf('.');
                    if (dotIndex == -1)
                    {
                        throw new ReplicatorException(
                                "There was a problem parsing the MASTER STATUS filename");
                    }

                    currentResourceEventId = fileName.substring(dotIndex + 1)
                            + ":" + masterStatus.getString("Position");
                }
                else
                {
                    throw new ReplicatorException(
                            "Unable to determine the current event id");
                }
            }
            catch (SQLException e)
            {
                throw new ReplicatorException(e);
            }
            finally
            {
                try
                {
                    masterStatus.close();
                }
                catch (SQLException e)
                {
                    throw new ReplicatorException(e);
                }
            }
        }

        return currentResourceEventId;
    }
}
