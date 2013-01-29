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
 * Contributor(s): Linas Virbalas, Stephane Giron
 */

package com.continuent.tungsten.replicator.applier.batch;

import java.io.File;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.exec.ProcessExecutor;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.Table;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * Represents a class to execute a native batch script.
 */
public class NativeScriptExecutor implements ScriptExecutor
{
    private static Logger logger      = Logger.getLogger(NativeScriptExecutor.class);

    // Location of merge command script and script itself.
    private String        script;
    private BatchScript   mergeScript = new BatchScript();

    // Whether we are to log script commands as they execute.
    private boolean       showCommands;

    // DBMS connection and statement.
    private Database      connection;
    private SqlWrapper    connectionWrapper;

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.batch.ScriptExecutor#setConnection(com.continuent.tungsten.replicator.database.Database)
     */
    @Override
    public void setConnection(Database connection)
    {
        this.connection = connection;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.batch.ScriptExecutor#setScript(java.lang.String)
     */
    @Override
    public void setScript(String script)
    {
        this.script = script;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.batch.ScriptExecutor#setShowCommands(boolean)
     */
    @Override
    public void setShowCommands(boolean showCommands)
    {
        this.showCommands = showCommands;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        // Nothing to do for now.
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.batch.ScriptExecutor#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    @Override
    public void prepare(PluginContext context) throws ReplicatorException
    {
        // Initialize script for merge operations.
        mergeScript = new BatchScript();
        mergeScript.load(new File(script));

        // Create a connection wrapper to provide SQL capabilities.
        try
        {
            connectionWrapper = new SqlWrapper(connection);
        }
        catch (SQLException e)
        {
            throw new ReplicatorException(
                    "Unable to initialize JDBC connection for load script: script="
                            + script + " message=" + e.getMessage(), e);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.batch.ScriptExecutor#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    @Override
    public void release(PluginContext context)
    {
        // Release SQL resources.
        if (connectionWrapper != null)
        {
            connectionWrapper.close();
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.batch.ScriptExecutor#execute(com.continuent.tungsten.replicator.applier.batch.CsvInfo)
     */
    @Override
    public void execute(CsvInfo info) throws ReplicatorException
    {
        Table base = info.baseTableMetadata;
        Table stage = info.stageTableMetadata;
        if (logger.isDebugEnabled())
        {
            logger.debug("Merging from stage table: "
                    + stage.fullyQualifiedName());
        }

        // Get the commands(s) to merge from stage table to base table.
        Map<String, String> parameters = info.getSqlParameters();
        List<BatchCommand> commands = mergeScript
                .getParameterizedScript(parameters);

        // Execute merge commands one by one.
        for (BatchCommand command : commands)
        {
            if (logger.isDebugEnabled())
            {
                logger.debug("Executing merge command: " + command.toString());
            }
            String commandText = command.getCommand();
            if (this.showCommands)
            {
                logger.info("Batch Command: " + commandText);
            }

            // Process command.
            long start = System.currentTimeMillis();
            if (commandText.startsWith("!"))
            {
                // Check for "bang" with no command following...
                if (commandText.length() <= 1)
                {
                    // This must be ignored.
                    continue;
                }

                String osCommandText = commandText.substring(1);
                String[] osArray = {"sh", "-c", osCommandText};
                ProcessExecutor pe = new ProcessExecutor();
                pe.setCommands(osArray);
                if (logger.isDebugEnabled())
                {
                    logger.debug("Executing OS command: " + osCommandText);
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
                    logger.error("OS command failed: command=" + osCommandText
                            + " rc=" + pe.getExitValue() + " stdout="
                            + pe.getStdout() + " stderr=" + pe.getStderr());
                    throw new ReplicatorException("OS command failed: command="
                            + osCommandText);
                }
            }
            else
            {
                // SQL command.
                try
                {
                    int rows = connectionWrapper.execute(commandText);
                    if (logger.isDebugEnabled())
                    {
                        logger.debug("SQL execution completed: rows updated="
                                + rows);
                    }
                }
                catch (SQLException e)
                {
                    ReplicatorException re = new ReplicatorException(
                            "Unable to merge data to base table: "
                                    + base.fullyQualifiedName(), e);
                    re.setExtraData(command.toString());
                    throw re;
                }
            }

            double interval = (System.currentTimeMillis() - start) / 1000.0;
            if (logger.isDebugEnabled())
            {
                logger.debug("Execution completed: duration=" + interval + "s");
            }
        }

    }
}