/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2010-2012 Continuent Inc.
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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * Implements an in-memory queue store that applies events from multiple task
 * threads (i.e., channels) into a single centralized queue. The queue has
 * transactional semantics in the sense that it buffers events from each task
 * thread and them applies them in a single operation upon commit. This is
 * helpful for detecting out-of-order apply due to errors in the synchronization
 * of parallel tasks.
 * <p/>
 * To further simulate DBMS behavior this queue supports commit actions, which
 * may perform arbitrary actions such as delaying or throwing exceptions.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class InMemoryTransactionalQueue implements Store
{
    private static Logger                              logger        = Logger.getLogger(InMemoryTransactionalQueue.class);
    private String                                     name;
    private int                                        partitions    = 1;
    private int                                        maxSize       = 1;
    private int                                        commitTimeout = 30;

    private BlockingQueue<ReplDBMSEvent>               serialQueue;
    private List<ConcurrentLinkedQueue<ReplDBMSEvent>> taskQueues;
    private ReplDBMSHeader[]                           lastHeader;
    private volatile long                              eventCount    = 0;
    private volatile long                              commitCount   = 0;
    private CommitAction                               commitAction;

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

    /** Maximum number of events permitted in serial queue. */
    public int getMaxSize()
    {
        return maxSize;
    }

    public void setMaxSize(int size)
    {
        this.maxSize = size;
    }

    /** Number of partitions supported by the queue. */
    public int getPartitions()
    {
        return partitions;
    }

    public void setPartitions(int partitions)
    {
        this.partitions = partitions;
    }

    /** Maximum commit timeout (fails after this) in seconds. */
    public int getCommitTimeout()
    {
        return commitTimeout;
    }

    public void setCommitTimeout(int commitTimeout)
    {
        this.commitTimeout = commitTimeout;
    }

    /**
     * An action to invoke prior to commit.
     */
    public CommitAction getCommitAction()
    {
        return commitAction;
    }

    public void setCommitAction(CommitAction commitAction)
    {
        this.commitAction = commitAction;
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
    public long getMaxStoredSeqno()
    {
        return -1;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.storage.Store#getMinStoredSeqno()
     */
    public long getMinStoredSeqno()
    {
        return 0;
    }

    /**
     * Puts an event in the local task queue, where it is buffered in order
     * until commit or rollback.
     */
    public void put(int taskId, ReplDBMSEvent event)
            throws InterruptedException
    {
        if (logger.isDebugEnabled())
        {
            logger.debug("Commit to task queue: taskId=" + taskId + " seqno="
                    + event.getSeqno() + " shardId=" + event.getShardId());
        }
        ConcurrentLinkedQueue<ReplDBMSEvent> queue = taskQueues.get(taskId);

        // Following operations are linked, hence we synchronize.
        synchronized (queue)
        {
            queue.add(event);
            eventCount++;
        }

        if (logger.isDebugEnabled())
        {
            if (eventCount % 10000 == 0)
                logger.debug("Transactional queue store: events=" + eventCount
                        + " xacts=" + commitCount);
        }
    }

    /**
     * Commits all pending events for a particular task.
     * 
     * @throws InterruptedException
     */
    public void commit(int taskId) throws InterruptedException,
            ReplicatorException
    {
        ConcurrentLinkedQueue<ReplDBMSEvent> queue = taskQueues.get(taskId);
        if (queue.size() == 0)
        {
            if (logger.isDebugEnabled())
            {
                logger.debug("Empty commit on serial queue: taskId=" + taskId);
            }
            return;
        }

        // Perform commit action. This should not be serialized to avoid
        // blocking other tasks.
        if (commitAction != null)
            commitAction.execute(taskId);

        // Copy events to serial queue. This must be serialized
        // on the serialQueue to ensure atomicity of updates.
        synchronized (serialQueue)
        {
            while (queue.peek() != null)
            {
                ReplDBMSEvent event = queue.poll();
                boolean ok = serialQueue.offer(event, commitTimeout,
                        TimeUnit.SECONDS);
                if (!ok)
                {
                    throw new ReplicatorException("Commit timed out: taskId="
                            + taskId + " timeout=" + commitTimeout);
                }
                if (logger.isDebugEnabled())
                {
                    logger.debug("Committing on serial queue: taskId=" + taskId
                            + " seqno=" + event.getSeqno() + " shardId="
                            + event.getShardId());
                }
            }
            queue.clear();
            commitCount++;
        }
    }

    /**
     * Rolls back pending events for a particular task.
     * 
     * @throws InterruptedException
     */
    public void rollback(int taskId)
    {
        ConcurrentLinkedQueue<ReplDBMSEvent> queue = taskQueues.get(taskId);
        synchronized (queue)
        {
            if (queue.size() > 0)
            {
                queue.clear();
            }
        }
    }

    /**
     * Removes and returns next event from the queue, blocking if empty.
     */
    public ReplDBMSEvent get() throws InterruptedException
    {
        return serialQueue.take();
    }

    /**
     * Returns but does not remove next event from the queue if it exists or
     * returns null if queue is empty.
     */
    public ReplDBMSEvent peek()
    {
        return serialQueue.peek();
    }

    /**
     * Returns the current serial queue size.
     */
    public int size()
    {
        return serialQueue.size();
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
        // Create serialized queue.
        serialQueue = new LinkedBlockingQueue<ReplDBMSEvent>(maxSize);

        // Create per-task buffers.
        taskQueues = new ArrayList<ConcurrentLinkedQueue<ReplDBMSEvent>>(
                partitions);
        for (int i = 0; i < partitions; i++)
        {
            taskQueues.add(new ConcurrentLinkedQueue<ReplDBMSEvent>());
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
        serialQueue = null;
        taskQueues = null;
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
        props.setLong("eventCount", this.eventCount);
        props.setLong("commitCount", this.commitCount);
        return props;
    }
}