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
 * Contributor(s): Stephane Giron
 */

package com.continuent.tungsten.replicator.storage;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.applier.Applier;
import com.continuent.tungsten.replicator.consistency.ConsistencyException;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.extractor.Extractor;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * Implements Extractor and Applier interfaces for an in-memory queue.
 *
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */

public class InMemoryQueueAdapter implements Extractor, Applier
{
    private static Logger      logger = Logger.getLogger(InMemoryQueueAdapter.class);
    private String             storeName;
    private InMemoryQueueStore queueStore;

    /**
     * Instantiate the adapter.
     */
    public InMemoryQueueAdapter()
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
     * @see com.continuent.tungsten.replicator.extractor.Extractor#extract()
     */
    public ReplDBMSEvent extract() throws ReplicatorException,
            InterruptedException
    {
        return queueStore.get();
    }

    /**
     * {@inheritDoc}
     *
     * @see com.continuent.tungsten.replicator.extractor.Extractor#getCurrentResourceEventId()
     */
    public String getCurrentResourceEventId() throws ReplicatorException,
            InterruptedException
    {
        ReplDBMSEvent event = queueStore.peek();
        if (event == null)
            return null;
        else
            return event.getEventId();
    }

    /**
     * Returns true if the queue has more events. {@inheritDoc}
     *
     * @see com.continuent.tungsten.replicator.extractor.Extractor#hasMoreEvents()
     */
    public boolean hasMoreEvents()
    {
        return queueStore.size() > 0;
    }

    /**
     * Store the header so that it can be propagated back through the pipeline
     * for restart. {@inheritDoc}
     *
     * @see com.continuent.tungsten.replicator.extractor.Extractor#setLastEvent(com.continuent.tungsten.replicator.event.ReplDBMSHeader)
     */
    public void setLastEvent(ReplDBMSHeader header) throws ReplicatorException
    {
        queueStore.setLastHeader(header);
    }

    /**
     * Ignored for now as in-memory queues do not extract. {@inheritDoc}
     *
     * @see com.continuent.tungsten.replicator.extractor.Extractor#setLastEventId(java.lang.String)
     */
    public void setLastEventId(String eventId) throws ReplicatorException
    {
        logger.warn("Attempt to set last event ID on queue storage: " + eventId);
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
            queueStore = (InMemoryQueueStore) context.getStore(storeName);
        }
        catch (ClassCastException e)
        {
            throw new ReplicatorException(
                    "Invalid storage class; configuration may be in error: "
                            + context.getStore(storeName).getClass().getName());
        }
        if (queueStore == null)
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
        queueStore = null;
    }

    /**
     * {@inheritDoc}
     *
     * @see com.continuent.tungsten.replicator.applier.Applier#apply(ReplDBMSEvent,
     *      boolean, boolean, boolean)
     */
    public void apply(ReplDBMSEvent event, boolean doCommit, boolean doRollback, boolean syncTHL)
            throws ReplicatorException, ConsistencyException, InterruptedException
    {
        queueStore.put(event);
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
        queueStore.setLastHeader(header);
    }

    /**
     * This method is meaningless for an in-memory queue, which is
     * non-transactional. {@inheritDoc}
     *
     * @see com.continuent.tungsten.replicator.applier.Applier#commit()
     */
    public void commit() throws ReplicatorException, InterruptedException
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
     * Return the header, which should have been place here by an extractor
     * during restart. {@inheritDoc}
     *
     * @see com.continuent.tungsten.replicator.applier.Applier#getLastEvent()
     */
    public ReplDBMSHeader getLastEvent() throws ReplicatorException,
            InterruptedException
    {
        return queueStore.getLastHeader();
    }
}
