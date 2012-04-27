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

import java.io.IOException;

import org.apache.log4j.Logger;

import com.continuent.tungsten.commons.cluster.resource.OpenReplicatorParams;
import com.continuent.tungsten.replicator.InSequenceNotification;
import com.continuent.tungsten.replicator.OutOfSequenceNotification;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.conf.ReplicatorConf;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSFilteredEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.event.ReplEvent;
import com.continuent.tungsten.replicator.extractor.Extractor;
import com.continuent.tungsten.replicator.extractor.ExtractorException;
import com.continuent.tungsten.replicator.plugin.PluginContext;
import com.continuent.tungsten.replicator.plugin.PluginLoader;
import com.continuent.tungsten.replicator.plugin.ShutdownHook;

/**
 * Implements an extractor to pull events from a remote THL.
 * <p/>
 * This class has specialized concurrency requirements as there is a potential
 * race condition to close connections within the task thread and thread trying
 * to shut down the pipeline. The race arises due the fact that connections may
 * hang when connecting or reading from a connection to a dropped interface and
 * do not accept interrupts. They need to be interrupted by closing the
 * connection. For this reason, closing the connection is synchronized.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class RemoteTHLExtractor implements Extractor, ShutdownHook
{
    private static Logger  logger             = Logger.getLogger(RemoteTHLExtractor.class);

    // Properties.
    private String         connectUri;
    private int            resetPeriod        = 1;
    private boolean        checkSerialization = true;
    private int            heartbeatMillis    = 3000;

    // Connection control variables.
    private PluginContext  pluginContext;
    private ReplDBMSHeader lastEvent;
    private String         lastEventId;
    private Connector      conn;

    private ReplEvent      pendingEvent;

    // Set to show that we have been shut down.
    private volatile boolean shutdown           = false;

    /**
     * Create Connector instance.
     */
    public RemoteTHLExtractor()
    {
    }

    public String getConnectUri()
    {
        return connectUri;
    }

    /**
     * Set the URI of the store to which we connect.
     * 
     * @param connectUri
     */
    public void setConnectUri(String connectUri)
    {
        this.connectUri = connectUri;
    }

    public int getResetPeriod()
    {
        return resetPeriod;
    }

    /**
     * Set the number of iterations before resetting the communications stream.
     * Higher values use more memory but are more efficient.
     */
    public void setResetPeriod(int resetPeriod)
    {
        this.resetPeriod = resetPeriod;
    }

    public boolean isCheckSerialization()
    {
        return checkSerialization;
    }

    /**
     * If true, check epoch number and sequence number of last event we have
     * received.
     * 
     * @param checkSerialization
     */
    public void setCheckSerialization(boolean checkSerialization)
    {
        this.checkSerialization = checkSerialization;
    }

    public int getHeartbeatInterval()
    {
        return heartbeatMillis;
    }

    /**
     * Sets the interval for sending heartbeat events from server to avoid
     * TCP/IP timeout on server connection.
     */
    public void setHeartbeatInterval(int heartbeatMillis)
    {
        this.heartbeatMillis = heartbeatMillis;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.Extractor#extract()
     */
    public ReplDBMSEvent extract() throws ReplicatorException,
            InterruptedException
    {
        try
        {
            // Open the connector if it is not yet open.
            if (conn == null)
            {
                openConnector();
            }

            // Fetch the event.
            ReplEvent replEvent = null;
            while (replEvent == null && !shutdown)
            {
                // If we have a pending event from an earlier read, return that.
                if (pendingEvent != null)
                {
                    replEvent = pendingEvent;
                    pendingEvent = null;
                    break;
                }

                long seqno = 0;
                try
                {
                    if (lastEvent != null)
                        if (lastEvent.getLastFrag())
                        {
                            if (lastEvent instanceof ReplDBMSFilteredEvent)
                            {
                                ReplDBMSFilteredEvent ev = (ReplDBMSFilteredEvent) lastEvent;
                                seqno = ev.getSeqnoEnd() + 1;
                            }
                            else
                                seqno = lastEvent.getSeqno() + 1;
                        }
                        else
                            seqno = lastEvent.getSeqno();
                    replEvent = conn.requestEvent(seqno);
                    if (replEvent == null)
                        continue;

                    // If the lastEventId was set, we have some housekeeping
                    // ahead of us.
                    if (lastEventId != null)
                    {
                        // Searching for lastEventId can cause skips in the
                        // log. If so, we need to insert a filter event to
                        // avoid breaks and return that first. Otherwise
                        // downstream stages will break due to sequence number
                        // gaps.
                        if (lastEvent != null && replEvent.getSeqno() > seqno)
                        {
                            pendingEvent = replEvent;
                            replEvent = new ReplDBMSFilteredEvent(lastEventId,
                                    seqno, replEvent.getSeqno() - 1, (short) 0);
                        }

                        // Next, clear the last event ID.
                        if (logger.isDebugEnabled())
                        {
                            logger.debug("Clearing last event ID: "
                                    + lastEventId);
                        }
                        lastEventId = null;
                    }
                }
                catch (IOException e)
                {
                    if (shutdown)
                    {
                        if (logger.isDebugEnabled())
                        {
                            logger.debug(
                                    "Ignoring exception after shutdown request",
                                    e);
                        }
                        logger.info("Connector read failed after shutdown; not attempting to reconnect");
                        break;
                    }
                    else
                    {
                        // If the connection dropped in the middle of a
                        // fragmented transaction, we need to ignore events that
                        // were already stored, otherwise it will generate an
                        // integrity constraint violation
                        reconnect();
                        continue;
                    }
                }

                // Ensure we have the right *sort* of replication event.
                if (replEvent != null && !(replEvent instanceof ReplDBMSEvent))
                    throw new ExtractorException(
                            "Unexpected event type: seqno =" + seqno + " type="
                                    + replEvent.getClass().getName());
            }

            // Remember which event we just read and ask for the next one.
            lastEvent = (ReplDBMSEvent) replEvent;
            return (ReplDBMSEvent) replEvent;
        }
        catch (THLException e)
        {
            // THLException messages are user-readable so just pass 'em along.
            throw new ExtractorException(e.getMessage(), e);
        }

    }

    /** Does not make sense for this extractor type. */
    public String getCurrentResourceEventId() throws ReplicatorException,
            InterruptedException
    {
        return null;
    }

    public boolean hasMoreEvents()
    {
        return false;
    }

    public void setLastEvent(ReplDBMSHeader event) throws ReplicatorException
    {
        lastEvent = event;
    }

    /**
     * Sets the last event ID for extraction. If this is set, we will request
     * (and receive) the first event from the master log that matches this
     * event.
     * 
     * @see com.continuent.tungsten.replicator.extractor.Extractor#setLastEventId(java.lang.String)
     */
    public void setLastEventId(String eventId) throws ReplicatorException
    {
        if (logger.isDebugEnabled())
            logger.debug("Set last event ID on remote THL extractor: "
                    + eventId);
        lastEventId = eventId;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException
    {
        // Store context for later.
        this.pluginContext = context;

        // Set the connect URI to a default if not already set.
        if (this.connectUri == null)
        {
            connectUri = context.getReplicatorProperties().get(
                    ReplicatorConf.MASTER_CONNECT_URI);
        }

        // See if we have an online option that overrides serialization
        // checking.
        if (pluginContext.getOnlineOptions().getBoolean(
                OpenReplicatorParams.FORCE))
        {
            if (checkSerialization)
            {
                logger.info("Force option enabled; log serialization checking is disabled");
                checkSerialization = false;
            }
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context) throws ReplicatorException
    {
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void release(PluginContext context) throws ReplicatorException
    {
        // Clearing the connection must be synchronized.
        // See concurrency note in class header comment.
        synchronized (this)
        {
            if (conn != null)
            {
                conn.close();
                // Do not clear variable. It can cause an NPR in the
                // openConnector() method which may still be attempting to
                // open a connection.
            }
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ShutdownHook#shutdown(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void shutdown(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        // Stop the connector.
        if (logger.isDebugEnabled())
        {
            logger.debug("Shutdown hook invoked; attempting to close connector");
        }
        shutdown = true;
        release(context);
    }

    // Reconnect a failed connection.
    private void reconnect() throws InterruptedException, ReplicatorException
    {
        synchronized (this)
        {
            if (conn != null)
            {
                conn.close();
                conn = null;
            }
        }
        
        // Reconnect after lost connection.
        logger.info("Connection to remote thl lost; reconnecting");
        pluginContext.getEventDispatcher().put(new OutOfSequenceNotification());
        openConnector();
    }

    // Open up the connector here.
    private void openConnector() throws ReplicatorException,
            InterruptedException
    {
        // Connect to remote THL.
        logger.info("Opening connection to master: " + connectUri);
        long retryCount = 0;
        for (;;)
        {
            try
            {
                // If we need to check serialization we must supply the seqno
                // and epoch.
                try
                {
                    conn = (Connector) PluginLoader
                            .load(pluginContext
                                    .getReplicatorProperties()
                                    .getString(
                                            ReplicatorConf.THL_PROTOCOL,
                                            ReplicatorConf.THL_PROTOCOL_DEFAULT,
                                            false));
                    conn.setURI(connectUri);
                    conn.setResetPeriod(resetPeriod);
                    conn.setHeartbeatMillis(heartbeatMillis);
                    conn.setLastEventId(this.lastEventId);
                    if (this.lastEvent == null
                            || this.checkSerialization == false)
                    {
                        conn.setLastSeqno(-1);
                        conn.setLastEpochNumber(-1);
                    }
                    else
                    {
                        conn.setLastSeqno(lastEvent.getSeqno());
                        conn.setLastEpochNumber(lastEvent.getEpochNumber());
                    }
                    conn.configure(pluginContext);
                    conn.prepare(pluginContext);
                }
                catch (ReplicatorException e)
                {
                    throw new THLException("Error while initializing plug-in ",
                            e);
                }

                conn.connect();
                break;
            }
            catch (IOException e)
            {
                // Sleep for 1 second per retry; report every 10 retries.
                synchronized (this)
                {
                    // Clearing the connection must be synchronized.
                    // See concurrency note in class header comment.
                    if (conn != null)
                    {
                        conn.close();
                        conn = null;
                    }
                }
                if ((retryCount % 10) == 0)
                {
                    logger.info("Waiting for master to become available: uri="
                            + connectUri + " retries=" + retryCount);
                }
                retryCount++;
                Thread.sleep(1000);
            }
        }

        // Announce the happy event and reset retry count.
        logger.info("Connected to master after " + retryCount + " retries");
        retryCount = 0;
        pluginContext.getEventDispatcher().put(new InSequenceNotification());
    }
}