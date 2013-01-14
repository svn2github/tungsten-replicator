/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2012 Continuent Inc.
 * Contact: tungsten@continuent.com
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

package com.continuent.tungsten.replicator.management.tungsten;

import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.conf.ReplicatorConf;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.DatabaseFactory;
import com.continuent.tungsten.replicator.database.Session;

/**
 * This class implements a task to kill non-Tungsten sessions on the DBMS and
 * return the number killed.
 */
public class PurgeTask implements Callable<Integer>
{
    private static Logger      logger = Logger.getLogger(PurgeTask.class);
    private TungstenProperties properties;

    /**
     * Create a new purge task with properties containing login information.
     * 
     * @param properties
     */
    public PurgeTask(TungstenProperties properties)
    {
        this.properties = properties;
    }

    /**
     * Execute a purge. This requires a privileged account.
     */
    public Integer call() throws Exception
    {
        // Isolate the JDBC URL. This requires a kludge to substitute the DBNAME
        // argument for a valid schema.
        Properties jProps = new Properties();
        jProps.setProperty("URL",
                properties.getString(ReplicatorConf.RESOURCE_JDBC_URL));
        jProps.setProperty("DBNAME", properties.getString(
                ReplicatorConf.METADATA_SCHEMA,
                ReplicatorConf.METADATA_SCHEMA_DEFAULT, true));
        TungstenProperties.substituteSystemValues(jProps);
        String url = jProps.getProperty("URL");

        // Get the login and password from properties
        String tungstenUser = properties
                .getString(ReplicatorConf.GLOBAL_DB_USER);
        String tungstenPw = properties
                .getString(ReplicatorConf.GLOBAL_DB_PASSWORD);
        Database db = DatabaseFactory.createDatabase(url, tungstenUser,
                tungstenPw, true);

        // Make sure we have user management so we can find and delete sessions.
        if (!db.supportsUserManagement())
        {
            logger.info("User management not supported for this database type; purge operation does nothing");
            return 0;
        }

        // Connect to the database.
        logger.info("Initiating purge of user sessions: tungsten user="
                + tungstenUser);
        try
        {
            db.connect(false);
        }
        catch (Exception e)
        {
            logger.error("Failed to connect to database server: "
                    + e.getMessage());
            throw e;
        }

        // List sessions and kill any session that does not match our login.
        int killed = 0;
        try
        {
            List<Session> sessions = db.listSessions();
            for (Session session : sessions)
            {
                String slogin = session.getLogin();
                if (!slogin.equals(tungstenUser))
                {
                    if (logger.isDebugEnabled())
                    {
                        logger.debug("Killing user session: " + session);
                    }
                    try
                    {
                        db.kill(session);
                        killed++;
                    }
                    catch (SQLException e)
                    {
                        String message = "Unable to terminate session: "
                                + session.toString() + " SQLException="
                                + e.getMessage();
                        logger.warn(message);
                        if (logger.isDebugEnabled())
                            logger.debug(message, e);
                    }
                }
            }
        }
        catch (ReplicatorException e)
        {
            logger.error("Purge task failed: " + e.getMessage());
            throw e;
        }
        catch (Exception e)
        {
            logger.error("Purge task failed: " + e.getMessage());
            throw e;
        }
        finally
        {
            logger.info("Number of user sessions purged: " + killed);
            db.close();
        }

        // Return final number of killed processes.
        return killed;
    }
}