/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2012-2013 Continuent Inc.
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

import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.csv.CsvInfo;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.Table;

/**
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class InfiniDBBatchApplier extends SimpleBatchApplier
{

    private static Logger logger = Logger.getLogger(SimpleBatchApplier.class);

    /**
     * Needs to be done in a transaction, otherwise cpimport will be unable to
     * get the lock on the stage table !
     * 
     * @param info
     * @throws ReplicatorException
     */
    protected void clearStageTable(CsvInfo info) throws ReplicatorException
    {
        Statement tmpStatement = null;

        Table table = info.stageTableMetadata;
        if (logger.isDebugEnabled())
        {
            logger.debug("Clearing InfiniDB stage table: "
                    + table.fullyQualifiedName());
        }

        // Generate and submit SQL command.
        String delete = "DELETE FROM " + table.fullyQualifiedName();
        if (logger.isDebugEnabled())
        {
            logger.debug("Executing delete command: " + delete);
        }
        try
        {
            Database conn = (Database) connections.get(0);
            tmpStatement = conn.createStatement();
            conn.setAutoCommit(false);
            int rowsLoaded = tmpStatement.executeUpdate(delete);
            conn.commit();
            conn.setAutoCommit(true);
            if (logger.isDebugEnabled())
            {
                logger.debug("Rows deleted: " + rowsLoaded);
            }
        }
        catch (Exception e)
        {
            ReplicatorException re = new ReplicatorException(
                    "Unable to delete data from stage table: "
                            + table.fullyQualifiedName(), e);
            re.setExtraData(delete);
            throw re;
        }
        finally
        {
            if (tmpStatement != null)
                try
                {
                    tmpStatement.close();
                }
                catch (SQLException e)
                {
                }
        }
    }
}
