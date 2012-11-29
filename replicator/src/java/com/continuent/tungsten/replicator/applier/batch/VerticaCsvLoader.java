/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2011-12 Continuent Inc.
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

package com.continuent.tungsten.replicator.applier.batch;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
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

/**
 * Implements a CSV loader that uses streaming interface available from the
 * Vertica JDBC driver.
 */
public class VerticaCsvLoader extends SqlCsvLoader
{
    private static Logger               logger              = Logger.getLogger(VerticaCsvLoader.class);

    // Properties.
    protected String                    stageLoadScript;

    // Cached load commands.
    protected SqlScriptGenerator        loadScriptGenerator = new SqlScriptGenerator();
    protected Map<String, List<String>> loadScripts         = new HashMap<String, List<String>>();

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
            // Create a statement for work on DBMS.
            try
            {
                statement = conn.createStatement();
            }
            catch (SQLException e)
            {
                throw new ReplicatorException(
                        "Unable to generate SQL statement for database connection",
                        e);
            }

            // Execute a loop for each load command.
            for (String loadCommand : loadCommands)
            {
                try
                {
                    long start = System.currentTimeMillis();

                    // Prepare a stream to staging file, which will be passed
                    // directly to Vertica.
                    File file = new File(info.file.getAbsolutePath());
                    fis = new FileInputStream(file);

                    // Throws java.sql.SQLException if a query execution fails.
                    ((com.vertica.PGStatement) statement).executeCopyIn(
                            loadCommand, fis);

                    commandCount++;
                    double interval = (System.currentTimeMillis() - start) / 1000.0;
                    if (logger.isDebugEnabled())
                    {
                        logger.debug("Execution completed: rows should've been updated="
                                + rowsToLoad + " duration=" + interval + "s");
                    }
                }
                catch (FileNotFoundException e)
                {
                    ReplicatorException re = new ReplicatorException(
                            "Unable to read staging file", e);
                    re.setExtraData(loadCommand);
                    throw re;
                }
                catch (SQLException e)
                {
                    ReplicatorException re = new ReplicatorException(
                            "Unable to execute load command", e);
                    re.setExtraData(loadCommand);
                    throw re;
                }
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