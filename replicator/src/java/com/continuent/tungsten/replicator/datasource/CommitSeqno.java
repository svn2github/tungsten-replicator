/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2013 Continuent Inc.
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
 * Denotes a catalog table that remembers the current replicator commit position
 * and retrieves it on demand. Here are the rules for use of implementations
 * <p>
 * <ul>
 * <li>Master - Masters update trep_commit_seqno whenever an extracted event is
 * stored.</li>
 * <li>Slave - Slaves update trep_commit_seqno whenever an event is applied</li>
 * </ul>
 * This interface is a generalization of the CommitSeqnoTable class.
 */
public interface CommitSeqno extends CatalogEntity
{
    /**
     * Set the number of channels to track. This is the basic mechanism to
     * support parallel replication.
     */
    public void setChannels(int channels);

    /**
     * Copies the single task 0 row left by a clean offline operation to add
     * rows for each task in multi-channel operation. This fails if task 0 does
     * not exist.
     * 
     * @throws ReplicatorException Thrown if the task ID 0 does not exist
     * @throws InterruptedException
     */
    public void expandTasks() throws ReplicatorException, InterruptedException;

    /**
     * Reduces the trep_commit_seqno table to task 0 entry *provided* there is a
     * task 0 row and provide all rows are at the same sequence number. This
     * operation allows the table to convert to a different number of apply
     * threads.
     */
    public boolean reduceTasks() throws ReplicatorException,
            InterruptedException;

    /**
     * Returns the header for the lowest committed sequence number or null if
     * none such can be found.
     */
    public ReplDBMSHeader minCommitSeqno() throws ReplicatorException,
            InterruptedException;

    /**
     * Returns an accessor suitable for performing operations for a particular
     * task ID.
     * 
     * @param taskId The stage task ID
     * @param conn A connection to the data source
     */
    public CommitSeqnoAccessor createAccessor(int taskId,
            UniversalConnection conn) throws ReplicatorException,
            InterruptedException;
}