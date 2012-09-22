/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2010-12 Continuent Inc.
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
 * Initial developer(s):  Robert Hodges
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.pipeline;

import com.continuent.tungsten.replicator.event.ReplControlEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.event.ReplEvent;

/**
 * Defines a basic schedule implementation that tracks watches on events and
 * task termination logic.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class SimpleSchedule implements Schedule
{
    private final Stage                 stage;
    private final SingleThreadStageTask task;

    /**
     * Creates a new schedule instance.
     * 
     * @param stage Stage to which this applies.
     */
    public SimpleSchedule(Stage stage, SingleThreadStageTask task)
    {
        this.stage = stage;
        this.task = task;
    }

    /**
     * {@inheritDoc}
     * 
     * @throws InterruptedException
     * @see com.continuent.tungsten.replicator.pipeline.Schedule#advise(com.continuent.tungsten.replicator.event.ReplEvent)
     */
    public int advise(ReplEvent replEvent) throws InterruptedException
    {
        // Process events by type.
        if (replEvent instanceof ReplDBMSEvent)
        {
            // For normal transactions just record and decide whether to skip.
            ReplDBMSEvent event = (ReplDBMSEvent) replEvent;
            setLastProcessedEvent(event);
            if (stage.getProgressTracker().skip(event))
                return CONTINUE_NEXT_COMMIT;
            else
                return PROCEED;
        }
        else if (replEvent instanceof ReplControlEvent)
        {
            // For control events decide whether to sync or stop.
            ReplControlEvent controlEvent = (ReplControlEvent) replEvent;
            if (controlEvent.getEventType() == ReplControlEvent.STOP)
                return QUIT;
            else if (controlEvent.getEventType() == ReplControlEvent.SYNC)
            {
                ReplDBMSHeader syncEvent = controlEvent.getHeader();
                stage.getProgressTracker().setLastProcessedEvent(
                        task.getTaskId(), syncEvent);
                return CONTINUE_NEXT_COMMIT;
            }
            else
                throw new RuntimeException("Unsupported control type: "
                        + controlEvent.getEventType());
        }
        else
            throw new RuntimeException("Unsupported event type: "
                    + replEvent.getClass().toString());
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.pipeline.Schedule#isCancelled()
     */
    public synchronized boolean isCancelled()
    {
        return stage.getProgressTracker().isCancelled(task.getTaskId());
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.pipeline.Schedule#setLastProcessedEvent(com.continuent.tungsten.replicator.event.ReplDBMSHeader)
     */
    public synchronized void setLastProcessedEvent(ReplDBMSHeader event)
            throws InterruptedException
    {
        stage.getProgressTracker().setLastProcessedEvent(task.getTaskId(),
                event);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.pipeline.Schedule#commit()
     */
    public void commit() throws InterruptedException
    {
        stage.getProgressTracker().commit(task.getTaskId());
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.pipeline.Schedule#taskEnd()
     */
    public synchronized void taskEnd()
    {
        stage.getTaskGroup().reportTaskShutdown(Thread.currentThread(), task);
    }

    /**
     * Skips the given event
     * 
     * @see StageProgressTracker#skip(ReplDBMSEvent)
     */
    public synchronized boolean skip(ReplDBMSEvent event)
            throws InterruptedException
    {
        return stage.getProgressTracker().skip(event);
    }

    /**
     * Signal that task has been cancelled. Causes the isCancelled() call to
     * return true.
     */
    public synchronized void cancel()
    {
        stage.getProgressTracker().cancel(task.getTaskId());
    }
}
