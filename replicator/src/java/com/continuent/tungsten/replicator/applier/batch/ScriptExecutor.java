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

package com.continuent.tungsten.replicator.applier.batch;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.plugin.ReplicatorPlugin;

/**
 * Denotes a class capable of executing a batch load script.  This interface
 * conforms to conventions for replicator plugins.  
 */
public interface ScriptExecutor extends ReplicatorPlugin
{
    /** Sets the DBMS connection. */
    public abstract void setConnection(Database connection);

    /** Sets the script name. */
    public abstract void setScript(String script);

    /** If set to true, show commands as they execute. */
    public abstract void setShowCommands(boolean showCommands);

    /**
     * Executes the script for a specific table.
     * 
     * @param info Information about the table to be loaded and source CSV file
     * @throws ReplicatorException Thrown if load operation fails
     */
    public abstract void execute(CsvInfo info) throws ReplicatorException;

}