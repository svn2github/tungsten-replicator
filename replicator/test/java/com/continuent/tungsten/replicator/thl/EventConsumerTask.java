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

package com.continuent.tungsten.replicator.thl;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.storage.InMemoryTransactionalQueue;

/**
 * Consumes transactions from a queue, keeping count of the transactions read as
 * well as the maximum sequence number.
 */
public class EventConsumerTask implements Runnable
{
    private static Logger                    logger   = Logger.getLogger(EventConsumerTask.class);

    // Parameters.
    private final InMemoryTransactionalQueue queue;
    private final long                       expectedEvents;

    // Number of transactions & events generated.
    private volatile int                     events   = 0;
    private volatile long                    maxSeqno = -1;
    private volatile Exception               exception;
    private volatile boolean                 done     = false;

    /**
     * Instantiates a consumer task.
     * 
     * @param queue Queue from which to read transactions
     */
    public EventConsumerTask(InMemoryTransactionalQueue queue,
            long expectedEvents)
    {
        this.queue = queue;
        this.expectedEvents = expectedEvents;
    }

    public int getEvents()
    {
        return events;
    }

    public long getMaxSeqno()
    {
        return maxSeqno;
    }

    public Exception getException()
    {
        return exception;
    }

    public boolean isDone()
    {
        return done;
    }

    /**
     * Run until there are no more events.
     * 
     * @see java.lang.Runnable#run()
     */
    public void run()
    {
        try
        {
            // Read events from queue.
            while (events < expectedEvents)
            {
                ReplDBMSEvent rde = queue.get();
                long seqno = rde.getSeqno();
                if (seqno > maxSeqno)
                    maxSeqno = seqno;
                events++;
                if ((events % 10000) == 0)
                {
                    logger.info("Consuming events: events=" + events
                            + " maxSeqno=" + maxSeqno);
                }
            }
            logger.info("Finished reading events from queue: events=" + events
                    + " maxSeqno=" + maxSeqno);
        }
        catch (InterruptedException e)
        {
            logger.info("Event consumer task loop interrupted");
        }
        catch (Exception e)
        {
            logger.error("Consumer loop failed!", e);
            exception = e;
        }
        catch (Throwable t)
        {
            logger.error("Consumer loop failed!", t);
        }
        finally
        {
            done = true;
        }
    }
}