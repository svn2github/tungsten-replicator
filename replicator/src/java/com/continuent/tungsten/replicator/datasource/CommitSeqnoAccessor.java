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
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;

/**
 * Denotes an accessor for stage tasks to update metadata in the CommitSeqno
 * table. This performs operations that may need to be integrated with
 * transactions.
 */
public interface CommitSeqnoAccessor
{
    /**
     * Set the task ID for this accessor.
     */
    public void setTaskId(int taskId);

    /**
     * Prepare for use. This method is assumed to allocate any required
     * resources
     * 
     * @throws ReplicatorException Thrown if resource allocation fails
     */
    public void prepare() throws ReplicatorException, InterruptedException;

    /**
     * Release all resources. Clients must call this to avoid resource leaks.
     * 
     * @throws ReplicatorException Thrown if resource deallocation fails
     */
    public void close() throws ReplicatorException, InterruptedException;

    /**
     * Updates the last committed seqno for a single channel. This is a client
     * call used by appliers to mark the restart position.
     */
    public void updateLastCommitSeqno(ReplDBMSHeader header, long appliedLatency)
            throws ReplicatorException, InterruptedException;

    /**
     * Fetches header data for last committed transaction for a particular
     * channel. This is a client call to get the restart position.
     */
    public ReplDBMSHeader lastCommitSeqno() throws ReplicatorException,
            InterruptedException;
}