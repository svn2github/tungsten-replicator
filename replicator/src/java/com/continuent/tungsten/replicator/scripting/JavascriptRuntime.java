/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2013-2014 Continuent Inc.
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
 * Contributor(s): Linas Virbalas
 */

package com.continuent.tungsten.replicator.scripting;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.exec.ProcessExecutor;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.datasource.DataSourceService;
import com.continuent.tungsten.replicator.datasource.UniversalConnection;
import com.continuent.tungsten.replicator.datasource.UniversalDataSource;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * Implements a runtime that can be provided to Javascript scripts with useful
 * functions like launching an OS process, getting access to the replicator
 * context, or allocating connections to data sources. This context has the
 * notion of a "default" data source since many scripts work with only a single
 * data source, whose name is known in advance.
 * <p/>
 * JavaScript runtime instances do not manage concurrency for the time being and
 * are not multi-thread safe. It is the responsibility of client code to ensure
 * proper synchronization.
 */
public class JavascriptRuntime
{
    private static Logger                                 logger        = Logger.getLogger(JavascriptRuntime.class);

    private PluginContext                                 context;
    private String                                        defaultDataSourceName;
    private Map<UniversalConnection, UniversalDataSource> connectionMap = new HashMap<UniversalConnection, UniversalDataSource>();

    /**
     * Creates a new runtime with current context.
     */
    public JavascriptRuntime(PluginContext context, String defaultDataSourceName)
    {
        this.context = context;
        this.defaultDataSourceName = defaultDataSourceName;
    }

    /**
     * Returns the context. This enables scripts to ask for full information
     * about the operating environment.
     */
    public PluginContext getContext()
    {
        return context;
    }

    /** Returns the default data source name or null if there is no default. */
    public String getDefaultDataSourceName()
    {
        return defaultDataSourceName;
    }

    /**
     * Returns a data source or null if the data source cannot be found.
     * 
     * @param name Name of the data source
     */
    public UniversalDataSource getDataSource(String name)
            throws ReplicatorException
    {
        DataSourceService dss = (DataSourceService) context.getService(name);
        if (dss == null)
            return null;
        else
            return dss.find(name);
    }

    /**
     * Returns a usable connection to a particular data source. The connection
     * is stored in a map for future reference.
     * 
     * @param datasourceName Name of the datasource.
     * @return A live connection
     * @throws ReplicatorException Thrown if there is an error creating the
     *             connection
     */
    public UniversalConnection connect(String datasourceName)
            throws ReplicatorException
    {
        UniversalDataSource uds = getDataSource(datasourceName);
        if (uds == null)
            return null;
        else
        {
            UniversalConnection connection = uds.getConnection();
            connectionMap.put(connection, uds);
            return connection;
        }
    }

    /**
     * Releases an existing connection by looking it up in the connection map
     * and releasing from the correct data source. This call is idempotent.
     * 
     * @param connection The connection to be released
     */
    public void disconnect(UniversalConnection connection)
    {
        // Remove and release if the connection exists.
        UniversalDataSource uds = connectionMap.remove(connection);
        if (uds != null)
        {
            uds.releaseConnection(connection);
        }
    }

    /**
     * Execute an OS command and return the result of stdout.
     * 
     * @param command Command to run
     * @return Returns output of the command in a string
     * @throws ReplicatorException Thrown if command execution fails
     */
    public String exec(String command) throws ReplicatorException
    {
        String[] osArray = {"sh", "-c", command};
        ProcessExecutor pe = new ProcessExecutor();
        pe.setCommands(osArray);
        if (logger.isDebugEnabled())
        {
            logger.debug("Executing OS command: " + command);
        }
        pe.run();
        if (logger.isDebugEnabled())
        {
            logger.debug("OS command stdout: " + pe.getStdout());
            logger.debug("OS command stderr: " + pe.getStderr());
            logger.debug("OS command exit value: " + pe.getExitValue());
        }
        if (!pe.isSuccessful())
        {
            String msg = "OS command failed: command=" + command + " rc="
                    + pe.getExitValue() + " stdout=" + pe.getStdout()
                    + " stderr=" + pe.getStderr();
            logger.error(msg);
            throw new ReplicatorException(msg);
        }
        return pe.getStdout();
    }

    /**
     * Substitutes parameter values from a Map into a command. This is used to
     * apply %%PARM%% style-parameters to a command template.
     * 
     * @param command Command template with parameter names
     * @param parameters Map containing name value pairs of parameters
     * @return Fully parameter
     */
    public String parameterize(String command, Map<String, String> parameters)
    {
        for (String key : parameters.keySet())
        {
            String value = parameters.get(key);
            command = command.replace(key, value);
        }
        return command;
    }

    /**
     * Supplies equivalent of sprintf function for Javascript callers to use, as
     * this is not easily available within Javascript itself.
     * 
     * @param format Printf-style format string
     * @param args Varargs values to substitute into the format.
     * @return Formatted string
     */
    public String sprintf(String format, Object... args)
    {
        return String.format(format, args);
    }
}