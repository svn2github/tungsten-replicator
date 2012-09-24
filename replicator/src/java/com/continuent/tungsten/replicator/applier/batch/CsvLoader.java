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
 * Initial developer(s): Robert Hodges
 * Contributor(s): 
 */

package com.continuent.tungsten.replicator.applier.batch;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.database.Database;

/**
 * Denotes a CsvLoader class, which handles loading of CsvData into a DBMS.
 * Implementations must support bean conventions including default constructor
 * and setters for all properties. Instances are auto-generated from the
 * replicator service properties file.
 */
public interface CsvLoader
{
    /**
     * Loads a CSV file to the DBMS using an appropriate mechanism for this DBMS
     * type.
     * 
     * @param conn JDBC connection to the DBMS
     * @param info Information on CSV file and table to which it applies
     * @param onLoadMisMatch How to handle a mismatch between actual rows loaded
     *            to DBMS and rows in the CSV file
     * @throws ReplicatorException Thrown if load fails
     */
    public abstract void load(Database conn, CsvInfo info,
            LoadMismatch onLoadMismatch) throws ReplicatorException;
}