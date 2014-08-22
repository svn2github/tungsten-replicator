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

package com.continuent.tungsten.replicator.datasource;

import java.io.BufferedWriter;

import com.continuent.tungsten.common.csv.CsvWriter;
import com.continuent.tungsten.replicator.ReplicatorException;

/**
 * Denotes a generic connection to a data source.
 */
public interface UniversalConnection
{
    /**
     * Returns a properly configured CsvWriter to generate CSV according to the
     * preferred conventions of this data source type.
     * 
     * @param writer A buffered writer to receive CSV output
     * @return A property configured CsvWriter instance
     */
    public CsvWriter getCsvWriter(BufferedWriter writer);

    /**
     * Commit the current transaction, which means to make a best effort to
     * ensure any data written to the connection are durable.
     * 
     * @throws Exception Thrown if the operation fails
     */
    public void commit() throws Exception;

    /**
     * Roll back the current transaction, which means to make a best effort to
     * ensure any data written to the connection since the last commit are
     * cleaned up.
     * 
     * @throws Exception Thrown if the operation fails
     */
    public void rollback() throws Exception;

    /**
     * Sets the commit semantics operations on the connection. This method has
     * no effect on connections that do not support transactions.
     * 
     * @param autoCommit If true each operation commits automatically; if false
     *            any further operations are enclosed in a transaction
     * @throws Exception Thrown if the operation fails
     */
    public void setAutoCommit(boolean autoCommit) throws Exception;

    /**
     * Sets the logging semantics on the connection if the data source is
     * logged. This method has no effect on data sources that do not support
     * logging.
     * 
     * @param logged If true operations on this connection are logged; if false
     *            connections are not logged
     */
    public void setLogged(boolean logged) throws ReplicatorException;

    /**
     * Sets the level of privileges that will be used on the connection. This
     * may be necessary to do root or superuser operations. This method is
     * harmless on data sources that do not have a notion of privileges.
     * 
     * @param privileged If true this connection is allowed to perform
     *            operations requiring super user operations; if false this
     *            operation is allowed only non-privileged operations
     */
    public void setPrivileged(boolean privileged);

    /**
     * Closes the connection and releases resource.
     */
    public void close();
}