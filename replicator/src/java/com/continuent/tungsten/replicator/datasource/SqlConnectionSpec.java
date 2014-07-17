/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2014 Continuent Inc.
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

/**
 * Denotes a class that can generate URLs for a specific DBMS type.
 */
public interface SqlConnectionSpec
{
    /** Returns the DBMS login. */
    public String getUser();

    /** Returns the password. */
    public String getPassword();

    /** Returns the DBMS schema for catalog tables. */
    public String getSchema();

    /** Returns true if we are updating and this is a privileged slave. */
    public boolean isPrivilegedSlaveUpdate();

    /** Returns true if we should log updates on the replication log. */
    public boolean isLogSlaveUpdates();

    /** Returns the DBMS table type. */
    public String getTableType();

    /**
     * Returns a URL to connect to the DBMS to which this specification applies.
     * 
     * @param createDB If true add option to create schema used by URL on
     *            initial connect. Ignored for DBMS types that do not support
     *            such an option.
     */
    public String createUrl(boolean createDB);
}