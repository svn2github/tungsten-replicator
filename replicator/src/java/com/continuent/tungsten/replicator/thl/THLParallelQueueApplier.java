/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2010-2011 Continuent Inc.
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

package com.continuent.tungsten.replicator.thl;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.applier.ApplierException;
import com.continuent.tungsten.replicator.applier.ParallelApplier;
import com.continuent.tungsten.replicator.consistency.ConsistencyException;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * Implements Applier interface for a THL parallel queue.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class THLParallelQueueApplier implements ParallelApplier
{
    private int              taskId = -1;
    private String           storeName;
    private THLParallelQueue thlParallelQueue;

    /**
     * Instantiate the adapter.
     */
    public THLParallelQueueApplier()
    {
    }

    public String getStoreName()
    {
        return storeName;
    }

    public void setStoreName(String storeName)
    {
        this.storeName = storeName;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException
    {
    }

    /**
     * Connect to underlying queue. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context) throws ReplicatorException
    {
        try
        {
            thlParallelQueue = (THLParallelQueue) context.getStore(storeName);
        }
        catch (ClassCastException e)
        {
            throw new ReplicatorException(
                    "Invalid storage class; configuration may be in error: "
                            + context.getStore(storeName).getClass().getName());
        }
        if (thlParallelQueue == null)
            throw new ReplicatorException(
                    "Unknown storage name; configuration may be in error: "
                            + storeName);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void release(PluginContext context) throws ReplicatorException
    {
        thlParallelQueue = null;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.Applier#apply(com.continuent.tungsten.replicator.event.ReplDBMSEvent,
     *      boolean, boolean, boolean)
     */
    public void apply(ReplDBMSEvent event, boolean doCommit,
            boolean doRollback, boolean syncTHL) throws ReplicatorException,
            ConsistencyException, InterruptedException
    {
        thlParallelQueue.put(taskId, event);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.Applier#updatePosition(com.continuent.tungsten.replicator.event.ReplDBMSHeader,
     *      boolean, boolean)
     */
    public void updatePosition(ReplDBMSHeader header, boolean doCommit,
            boolean syncTHL) throws ReplicatorException, InterruptedException
    {
        // This call does not mean anything for a parallel queue.
    }

    /**
     * This method is meaningless for an in-memory queue, which is
     * non-transactional. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.Applier#commit()
     */
    public void commit() throws ApplierException, InterruptedException
    {
    }

    /**
     * This method is meaningless for an in-memory queue, which is
     * non-transactional. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.Applier#rollback()
     */
    public void rollback() throws InterruptedException
    {
    }

    /**
     * Return the minimum header across all queues to ensure we do not miss
     * events on restart. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.Applier#getLastEvent()
     */
    public ReplDBMSHeader getLastEvent() throws ReplicatorException,
            InterruptedException
    {
        long minSeqno = Long.MAX_VALUE;
        ReplDBMSHeader minHeader = null;
        for (int i = 0; i < thlParallelQueue.getPartitions(); i++)
        {
            ReplDBMSHeader nextHeader = thlParallelQueue.getLastHeader(i);
            // Accept only a value header with a real sequence number as
            // the lowest starting sequence number.
            if (nextHeader == null)
                continue;
            else if (nextHeader.getSeqno() < 0)
                continue;
            else if (nextHeader.getSeqno() < minSeqno)
            {
                minHeader = nextHeader;
                minSeqno = minHeader.getSeqno();
            }
        }
        return minHeader;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.ParallelApplier#setTaskId(int)
     */
    public void setTaskId(int id) throws ApplierException
    {
        this.taskId = id;
    }
}