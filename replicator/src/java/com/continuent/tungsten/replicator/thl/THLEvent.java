/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2011 Continuent Inc.
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
 * Initial developer(s): Teemu Ollakka
 * Contributor(s): Robert Hodges
 */

package com.continuent.tungsten.replicator.thl;

import java.io.Serializable;
import java.sql.Timestamp;

import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplEvent;
import com.continuent.tungsten.replicator.event.ReplOptionParams;

/**
 * This class defines a THLEvent
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public class THLEvent implements Serializable
{
    static final long         serialVersionUID   = -1;

    /* Event types */
    /** Event carrying DBMS event information */
    static public final short REPL_DBMS_EVENT    = 0;

    /** This event is created each time node gets into master state */
    static public final short START_MASTER_EVENT = 1;

    /**
     * This event is created each time master node exits master state gracefully
     */
    static public final short STOP_MASTER_EVENT  = 2;

    /** Heartbeat event */
    static public final short HEARTBEAT_EVENT    = 3;

    private final long        seqno;
    private final short       fragno;
    private final boolean     lastFrag;
    private final String      sourceId;
    private final short       type;
    private final long        epochNumber;
    private final Timestamp   sourceTstamp;
    private final Timestamp   localEnqueueTstamp;
    private final Timestamp   processedTstamp;
    private String            comment;
    private final String      eventId;
    private final String      shardId;
    private final ReplEvent   event;

    /**
     * Creates a new <code>THLEvent</code> object with status set to COMPLETED.
     * 
     * @param eventId Event identifier
     * @param event ReplDBMSEvent
     */
    public THLEvent(String eventId, ReplDBMSEvent event)
    {
        this.seqno = event.getSeqno();
        this.fragno = event.getFragno();
        this.lastFrag = event.getLastFrag();
        this.sourceId = event.getSourceId();
        this.type = REPL_DBMS_EVENT;
        this.epochNumber = event.getEpochNumber();
        this.localEnqueueTstamp = null;
        this.sourceTstamp = event.getDBMSEvent().getSourceTstamp();
        this.processedTstamp = null;
        this.comment = null;
        this.eventId = eventId;
        this.shardId = event.getShardId();
        this.event = event;
    }

    /**
     * Creates a new <code>THLEvent</code> object
     * 
     * @param seqno Sequence number
     * @param fragno Fragment number
     * @param lastFrag Last fragment flag
     * @param sourceId Source identifier
     * @param type Event type
     * @param localEnqueueTstamp Local enqueue timestamp
     * @param sourceTstamp Source timestamp
     * @param processedTstamp Processed timestamp
     * @param eventId Event identifier
     * @param event Event
     */
    public THLEvent(long seqno, short fragno, boolean lastFrag,
            String sourceId, short type, long epochNumber,
            Timestamp localEnqueueTstamp, Timestamp sourceTstamp,
            Timestamp processedTstamp, String eventId, String shardId,
            ReplEvent event)
    {
        this.seqno = seqno;
        this.fragno = fragno;
        this.lastFrag = lastFrag;
        this.sourceId = sourceId;
        this.type = type;
        this.epochNumber = epochNumber;
        this.localEnqueueTstamp = localEnqueueTstamp;
        this.sourceTstamp = sourceTstamp;
        this.processedTstamp = processedTstamp;
        this.eventId = eventId;
        this.shardId = shardId;
        this.event = event;
    }

    /**
     * Get event sequence number.
     * 
     * @return Sequence number
     */
    public long getSeqno()
    {
        return seqno;
    }

    /**
     * Get event fragment number.
     * 
     * @return Fragment number
     */
    public short getFragno()
    {
        return fragno;
    }

    /**
     * Get last fragment flag.
     * 
     * @return Last fragment flag
     */
    public boolean getLastFrag()
    {
        return lastFrag;
    }

    /**
     * Get source identifier.
     * 
     * @return Source identifier
     */
    public String getSourceId()
    {
        return sourceId;
    }

    /**
     * Get event type.
     * 
     * @return Event type
     */
    public short getType()
    {
        return type;
    }

    /**
     * Get event epoch number.
     * 
     * @return Epoch number
     */
    public long getEpochNumber()
    {
        return epochNumber;
    }

    /**
     * Get local enqueue timestamp.
     * 
     * @return Local enqueue timestamp
     */
    public Timestamp getLocalEnqueueTstamp()
    {
        return localEnqueueTstamp;
    }

    /**
     * Get source timestamp.
     * 
     * @return Source timestamp
     */
    public Timestamp getSourceTstamp()
    {
        return sourceTstamp;
    }

    /**
     * Get processed timestamp.
     * 
     * @return Processed timestamp
     */
    public Timestamp getProcessedTstamp()
    {
        return processedTstamp;
    }

    /**
     * Get event comment.
     * 
     * @return Comment
     */
    public String getComment()
    {
        return comment;
    }

    /**
     * Get event identifier.
     * 
     * @return event's id
     */
    public String getEventId()
    {
        return eventId;
    }

    /**
     * Returns the shard ID.
     */
    public String getShardId()
    {
        if (shardId == null)
            return ReplOptionParams.SHARD_ID_UNKNOWN;
        else
            return shardId;
    }

    /**
     * Get associated replication.
     * 
     * @return associated ReplEvent
     */
    public ReplEvent getReplEvent()
    {
        return event;
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#toString()
     */
    public String toString()
    {
        String ret = "seqno=" + seqno;
        ret += " fragno=" + fragno;
        ret += " lastFrag=" + lastFrag;
        ret += " sourceId=" + sourceId;
        ret += " type=" + type;
        ret += " localEnqueueTstamp=" + localEnqueueTstamp;
        ret += " sourceTstamp=" + sourceTstamp;
        ret += " processedTstamp=" + processedTstamp;
        ret += " eventId=" + eventId;
        ret += " shardId=" + getShardId();
        ret += " event=" + event.toString();
        return ret;
    }
}
