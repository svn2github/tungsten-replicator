/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2010-2013 Continuent Inc.
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

package com.continuent.tungsten.replicator.storage;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * Implements an in-memory queue store with multiple queues. This is used for
 * testing other parallel queues where we need to simulate ability to apply in
 * parallel across a bunch of threads and tell what happened on each.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class InMemoryMultiQueue implements Store
{
    private static Logger                      logger           = Logger.getLogger(InMemoryMultiQueue.class);
    private String                             name;
    private int                                partitions       = 1;
    private int                                maxSize          = 1;

    private long                               minStored        = Long.MAX_VALUE;
    private long                               maxStored        = Long.MIN_VALUE;

    private List<BlockingQueue<ReplDBMSEvent>> queues;
    private ReplDBMSHeader[]                   lastHeader;
    private long                               transactionCount = 0;

    // Metadata tag used to recognize it's time to fail. If this is set in the 
    // event metadata we will fail.  
    private static final String                FAILURE_TAG      = "fail";

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.storage.Store#getName()
     */
    public String getName()
    {
        return name;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.storage.Store#setName(java.lang.String)
     */
    public void setName(String name)
    {
        this.name = name;
    }

    public int getMaxSize()
    {
        return maxSize;
    }

    public void setMaxSize(int size)
    {
        this.maxSize = size;
    }

    public int getPartitions()
    {
        return partitions;
    }

    public void setPartitions(int partitions)
    {
        this.partitions = partitions;
    }

    /** Sets the last header processed. This is required for restart. */
    public void setLastHeader(int taskId, ReplDBMSHeader header)
    {
        lastHeader[taskId] = header;
    }

    /** Returns the last header processed. */
    public ReplDBMSHeader getLastHeader(int taskId)
    {
        return lastHeader[taskId];
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.storage.Store#getMaxStoredSeqno()
     */
    public synchronized long getMaxStoredSeqno()
    {
        return this.maxStored;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.storage.Store#getMinStoredSeqno()
     */
    public synchronized long getMinStoredSeqno()
    {
        return this.minStored;
    }

    /**
     * Puts an event in the queue, blocking if it is full. This call fails if
     * failAll is true, which can be used to test error handling.
     */
    public void put(int taskId, ReplDBMSEvent event)
            throws InterruptedException, ReplicatorException
    {
        // See if we want to fail now.
        String failTag = event.getDBMSEvent().getMetadataOptionValue(
                FAILURE_TAG);
        if (failTag != null)
            throw new ReplicatorException("Failure triggered by " + FAILURE_TAG
                    + "=" + failTag);

        // Insert into the queue.
        queues.get(taskId).put(event);
        transactionCount++;
        if (logger.isDebugEnabled())
        {
            if (transactionCount % 10000 == 0)
                logger.debug("Queue store: xacts=" + transactionCount);
        }

        // Record the sequence number.
        synchronized (this)
        {
            long seqno = event.getSeqno();
            if (seqno < minStored)
                minStored = seqno;
            if (seqno > maxStored)
                maxStored = seqno;
        }
    }

    /**
     * Removes and returns next event from the queue, blocking if empty.
     */
    public ReplDBMSEvent get(int taskId) throws InterruptedException
    {
        return queues.get(taskId).take();
    }

    /**
     * Removes and returns next event from the queue, waiting up to specified
     * number of milliseconds. Returns null if nothing is read in this time.
     */
    public ReplDBMSEvent get(int taskId, long waitMillis)
            throws InterruptedException
    {
        return queues.get(taskId).poll(waitMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * Returns but does not remove next event from the queue if it exists or
     * returns null if queue is empty.
     */
    public ReplDBMSEvent peek(int taskId)
    {
        return queues.get(taskId).peek();
    }

    /**
     * Returns the current queue size.
     */
    public int size(int taskId)
    {
        return queues.get(taskId).size();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException
    {
        // Nothing to do.
    }

    /**
     * Allocate an in-memory queue. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context) throws ReplicatorException
    {
        queues = new ArrayList<BlockingQueue<ReplDBMSEvent>>(partitions);
        for (int i = 0; i < partitions; i++)
        {
            queues.add(new LinkedBlockingQueue<ReplDBMSEvent>(maxSize));
        }
        lastHeader = new ReplDBMSHeader[partitions];
    }

    /**
     * Release queue. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void release(PluginContext context) throws ReplicatorException
    {
        queues = null;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.storage.Store#status()
     */
    public TungstenProperties status()
    {
        TungstenProperties props = new TungstenProperties();
        props.setLong("maxSize", maxSize);
        props.setLong("eventCount", this.transactionCount);
        return props;
    }
}