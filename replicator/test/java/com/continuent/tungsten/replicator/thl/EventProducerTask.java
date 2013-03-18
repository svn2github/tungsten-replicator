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

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.thl.log.LogConnection;

/**
 * Runnable to create transactions for test and report the number thereof as
 * well as any exceptions.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class EventProducerTask implements Runnable
{
    private static Logger      logger   = Logger.getLogger(EventProducerTask.class);

    // Parameters.
    private EventProducer      producer;
    private THL                thl;

    // Number of transactions & events generated.
    private volatile int       events   = 0;
    private volatile long      maxSeqno = -1;
    private volatile Exception exception;
    private volatile boolean   done     = false;

    /**
     * Instantiates a generator task.
     * 
     * @param generator Generates transactions
     * @param thl Log
     */
    public EventProducerTask(EventProducer generator, THL thl)
    {
        this.producer = generator;
        this.thl = thl;
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
        LogConnection conn = null;
        try
        {
            // Connect to log.
            conn = thl.connect(false);

            // Write events from generator.
            ReplDBMSEvent rde;
            while ((rde = producer.nextEvent()) != null)
            {
                maxSeqno = rde.getSeqno();
                THLEvent thlEvent = new THLEvent(rde.getSourceId(), rde);
                conn.store(thlEvent, false);
                conn.commit();
                events++;
            }
            logger.info("Finished writing events to log: events=" + events
                    + " maxSeqno=" + maxSeqno);
        }
        catch (InterruptedException e)
        {
            logger.info("Event generator task loop interrupted");
        }
        catch (Exception e)
        {
            logger.error("Generation loop failed!", e);
            exception = e;
        }
        catch (Throwable t)
        {
            logger.error("Generation loop failed!", t);
        }
        finally
        {
            if (conn != null)
            {
                try
                {
                    thl.disconnect(conn);
                }
                catch (ReplicatorException e)
                {
                    logger.warn(
                            "Unable to disconnect from log after generating events",
                            e);
                }
            }
            done = true;
        }
    }
}