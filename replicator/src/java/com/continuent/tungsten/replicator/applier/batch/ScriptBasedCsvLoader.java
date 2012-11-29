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
 * Initial developer(s): Stephane Giron
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.applier.batch;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.SqlScriptGenerator;
import com.continuent.tungsten.replicator.database.Table;
import com.continuent.tungsten.replicator.util.ProcessHelper;

/**
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class ScriptBasedCsvLoader implements CsvLoader
{
    private static Logger               logger              = Logger.getLogger(SqlCsvLoader.class);

    // Properties.
    protected String                    stageLoadScript;

    // Cached load commands.
    protected SqlScriptGenerator        loadScriptGenerator = new SqlScriptGenerator();
    protected Map<String, List<String>> loadScripts         = new HashMap<String, List<String>>();

    ProcessHelper                       helper;

    public void setStageLoadScript(String stageLoadScript)
    {
        this.stageLoadScript = stageLoadScript;
    }

    public String getStageLoadScript()
    {
        return stageLoadScript;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.batch.CsvLoader#load(com.continuent.tungsten.replicator.database.Database,
     *      com.continuent.tungsten.replicator.applier.batch.CsvInfo,
     *      com.continuent.tungsten.replicator.applier.batch.LoadMismatch)
     */
    @Override
    public void load(Database conn, CsvInfo info, LoadMismatch onLoadMismatch)
            throws ReplicatorException
    {
        if (helper == null)
        {
            helper = new ProcessHelper();
            helper.configure();
        }

        int rowsToLoad = info.writer.getRowCount();

        // Generate the load command(s).
        Table base = info.baseTableMetadata;
        List<String> loadCommands = loadScripts.get(base.fullyQualifiedName());
        if (loadCommands == null)
        {
            loadScriptGenerator = SimpleBatchApplier
                    .initializeGenerator(stageLoadScript);
            // If we do not have load commands yet, generate them.
            Map<String, String> parameters = info.getSqlParameters();
            loadCommands = loadScriptGenerator
                    .getParameterizedScript(parameters);
            loadScripts.put(base.fullyQualifiedName(), loadCommands);
        }

        // Execute aforesaid load commands.
        int commandCount = 0;
        Statement statement = null;
        FileInputStream fis = null;
        try
        {
            // Execute a loop for each load command.
            for (String loadCommand : loadCommands)
            {
                logger.warn("Running command " + loadCommand);
                long start = System.currentTimeMillis();

                try
                {
                    File dumpFile = File.createTempFile("cpimport", null);
                    String[] cmd = loadCommand.split(" ");
                    helper.exec("Running cpimport", cmd,
                            null, dumpFile, null, false, false);
                }
                catch (IOException e)
                {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }

                commandCount++;
                double interval = (System.currentTimeMillis() - start) / 1000.0;
                if (logger.isDebugEnabled())
                {
                    logger.debug("Execution completed: rows should've been updated="
                            + rowsToLoad + " duration=" + interval + "s");
                }
                // }
                // catch (FileNotFoundException e)
                // {
                // ReplicatorException re = new ReplicatorException(
                // "Unable to read staging file", e);
                // re.setExtraData(loadCommand);
                // throw re;
                // }
                // catch (SQLException e)
                // {
                // ReplicatorException re = new ReplicatorException(
                // "Unable to execute load command", e);
                // re.setExtraData(loadCommand);
                // throw re;
                // }
            }
        }
        finally
        {
            // Release the statement if it was allocated.
            if (statement != null)
            {
                try
                {
                    statement.close();
                }
                catch (SQLException e)
                {
                }
            }
            // Release the file if it was allocated.
            try
            {
                if (fis != null)
                    fis.close();
            }
            catch (IOException e)
            {
            }
        }
    }
}
