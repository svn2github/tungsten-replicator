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
 * Initial developer(s): Linas Virbalas
 * Contributor(s): 
 */

package com.continuent.tungsten.replicator.applier.batch;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.database.Table;

/**
 * Implements an applier that bulk loads data into a Vertica database via stream
 * interface.
 * 
 * @author <a href="mailto:linas.virbalas@continuent.com">Linas Virbalas</a>
 * @version 1.0
 */
public class VerticaStreamBatchApplier extends SimpleBatchApplier
{
    private static Logger logger = Logger.getLogger(VerticaStreamBatchApplier.class);

    // Load an open CSV file.
    protected void load(CsvInfo info) throws ReplicatorException
    {
        if (logger.isDebugEnabled())
        {
            logger.debug("Loading CSV file: " + info.file.getAbsolutePath());
        }
        int rowsToLoad = info.writer.getRowCount();

        // Generate the load command(s).
        Table base = info.baseTableMetadata;
        List<String> loadCommands = loadScripts.get(base.fullyQualifiedName());
        if (loadCommands == null)
        {
            // If we do not have load commands yet, generate them.
            Map<String, String> parameters = info.getSqlParameters();
            loadCommands = loadScriptGenerator
                    .getParameterizedScript(parameters);
            loadScripts.put(base.fullyQualifiedName(), loadCommands);
        }

        // Execute aforesaid load commands.
        int commandCount = 0;
        for (String loadCommand : loadCommands)
        {
            if (logger.isDebugEnabled())
            {
                logger.debug("Executing load command: " + loadCommand);
            }
            FileInputStream fis = null;
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
            finally
            {
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

        // Delete the load file if we are done with it.
        if (cleanUpFiles && !info.file.delete())
        {
            logger.warn("Unable to delete load file: "
                    + info.file.getAbsolutePath());
        }
    }
}