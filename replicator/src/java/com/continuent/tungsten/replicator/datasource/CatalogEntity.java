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
 * Contributor(s): Stephane Giron
 */

package com.continuent.tungsten.replicator.datasource;

import com.continuent.tungsten.replicator.ReplicatorException;

/**
 * Denotes a catalog entity and specifies the contract for the catalog table
 * life-cycle.
 */
public interface CatalogEntity
{
    /**
     * Complete configuration. This is called after setters are invoked.
     * 
     * @throws ReplicatorException Thrown if configuration is incomplete or
     *             fails
     */
    public void configure() throws ReplicatorException, InterruptedException;

    /**
     * Prepare for use. This method is assumed to allocate any required
     * resources
     * 
     * @throws ReplicatorException Thrown if resource allocation fails
     */
    public void prepare() throws ReplicatorException, InterruptedException;

    /**
     * Release all resources. This is called before the table is deallocated.
     * 
     * @throws ReplicatorException Thrown if resource deallocation fails
     */
    public void release() throws ReplicatorException, InterruptedException;

    /**
     * Ensures all catalog data are present and properly initialized for use. If
     * data are absent, this call creates them. If they are present, this call
     * validates that they are ready for use.
     */
    public void initialize() throws ReplicatorException, InterruptedException;

    /**
     * Removes any and all catalog data. This is a dangerous call as it will
     * cause the replication service to lose all memory of its position.
     */
    public void clear() throws ReplicatorException, InterruptedException;
}