/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2011-2012 Continuent Inc.
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
 * Initial developer(s): Stephane Giron, Robert Hodges
 * Contributor(s): 
 */

package com.continuent.tungsten.replicator.prefetch;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.DatabaseFactory;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.event.ReplDBMSHeaderData;
import com.continuent.tungsten.replicator.plugin.PluginContext;
import com.continuent.tungsten.replicator.storage.InMemoryQueueStore;
import com.continuent.tungsten.replicator.thl.CommitSeqnoTable;

/**
 * Implements a specialized store for handling slave prefetch from another
 * replicator. This store coordinates restart at the current slave position and
 * implements logic to drop events that are not far enough ahead of the slave
 * position or have already been executed.
 */
public class PrefetchStore extends InMemoryQueueStore
{
    private static Logger     logger         = Logger.getLogger(PrefetchStore.class);

    // Prefetch store parameters.
    private String            user;
    private String            url;
    private String            password;
    private String            slaveCatalogSchema;

    private long              interval       = 1000;
    private int               maxTimeAhead   = 60;
    private int               minTimeAhead   = 0;
    private int               sleepTime      = 500;
    private boolean           allowAll       = false;

    // Database connection information.
    private Database          conn           = null;
    private PreparedStatement seqnoStatement = null;

    // Prefetch coordination information.

    // State information.
    enum PrefetchState
    {
        active, sleeping
    };

    // Prefetch stats.
    private long          totalEvents    = 0;
    private long          prefetchEvents = 0;
    private PrefetchState prefetchState;
    private long          startTimeMillis;
    private long          sleepTimeMillis;

    // Prefetch control information.
    private long          lastChecked    = 0;
    private long          slaveSeqno     = -1;
    private long          aheadMaxMillis;
    private long          aheadMinMillis;

    private long          slaveLatency;
    private long          prefetchLatency;

    /** Sets the JDBC URL to connect to the slave server. */
    public void setUrl(String url)
    {
        this.url = url;
    }

    public void setUser(String user)
    {
        this.user = user;
    }

    /** Sets the password of the database login to check slave position. */
    public void setPassword(String password)
    {
        this.password = password;
    }

    /**
     * Sets the catalog schema name of the slave for which we are prefetching.
     */
    public void setSlaveCatalogSchema(String slaveCatalogSchema)
    {
        this.slaveCatalogSchema = slaveCatalogSchema;
    }

    /**
     * Sets the number of milliseconds between slave position checks.
     * 
     * @param timeInMillis
     */
    public void setCheckInterval(long timeInMillis)
    {
        this.interval = timeInMillis;
    }

    /**
     * Sets the minimum number of seconds ahead of slave in order to accept an
     * event for prefetch. If an event is under this number we discard it.
     * 
     * @param aheadMinTime Minimum lead time in seconds
     */
    public void setMinTimeAhead(int aheadMinTime)
    {
        this.minTimeAhead = aheadMinTime;
    }

    /**
     * Sets the maximum number of seconds that event should be from the last
     * event applied by the slave. If we exceed this we sleep to let the slave
     * catch up.
     * 
     * @param aheadMaxTime Maximum lead time in seconds
     */
    public void setMaxTimeAhead(int aheadMaxTime)
    {
        this.maxTimeAhead = aheadMaxTime;
    }

    /**
     * Sets the number of milliseconds to sleep when we get too far ahead of the
     * slave.
     * 
     * @param sleepTime The sleepTime to set.
     */
    public void setSleepTime(int sleepTime)
    {
        this.sleepTime = sleepTime;
    }

    /**
     * Allow all events regardless of position of slave service we are tracking.
     * Used for debugging and to exercise prefetch applier code easily.
     */
    public void setAllowAll(boolean allowAll)
    {
        this.allowAll = allowAll;
    }

    /** Sets the last header processed. This is required for restart. */
    public void setLastHeader(ReplDBMSHeader header)
    {
        // Ignore last header from downstream stages.
    }

    /**
     * Returns the position of the slave on which we are handling prefetch.
     */
    public ReplDBMSHeader getLastHeader()
    {
        return this.getCurrentSlaveHeader();
    }

    /**
     * Puts an event in the queue, blocking if it is full.
     */
    public void put(ReplDBMSEvent event) throws InterruptedException,
            ReplicatorException
    {
        // See if we want the event and put it in the queue if so.
        if (filter(event) != null)
        {
            super.put(event);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.storage.InMemoryQueueStore#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException
    {
        super.configure(context);

        // Validate min and max ahead times.
        if (minTimeAhead >= 0)
            aheadMinMillis = minTimeAhead * 1000;
        else
            throw new ReplicatorException(
                    "Property minTimeAhead must be 0 or more: " + minTimeAhead);

        if (maxTimeAhead >= minTimeAhead)
            aheadMaxMillis = maxTimeAhead * 1000;
        else
            throw new ReplicatorException(
                    "Property maxTimeAhead equal to or greater than minTimeAhead: maxTimeAhead="
                            + maxTimeAhead + " minTimeAhead=" + minTimeAhead);
    }

    /**
     * Prepare prefetch store. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context) throws ReplicatorException
    {
        // Perform super-class prepare.
        super.prepare(context);

        // Print a warning if the allowAll flag is set.
        if (allowAll)
        {
            logger.warn("PrefetchStore allowAll property set--all transactions will be allowed");
        }

        logger.info("Preparing PrefetchStore for slave catalog schema: "
                + slaveCatalogSchema);
        // Load defaults for connection
        if (url == null)
            url = context.getJdbcUrl(null);
        if (user == null)
            user = context.getJdbcUser();
        if (password == null)
            password = context.getJdbcPassword();

        // Connect.
        try
        {
            conn = DatabaseFactory.createDatabase(url, user, password);
            conn.connect();

            seqnoStatement = conn
                    .prepareStatement("select seqno, fragno, last_Frag, source_id, epoch_number, eventid, applied_latency from "
                            + slaveCatalogSchema
                            + "."
                            + CommitSeqnoTable.TABLE_NAME);
        }
        catch (SQLException e)
        {
            throw new ReplicatorException(e);
        }

        // Show that we have started.
        startTimeMillis = System.currentTimeMillis();
        prefetchState = PrefetchState.active;
    }

    /**
     * Release queue. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void release(PluginContext context) throws ReplicatorException
    {
        queue = null;
        if (conn != null)
        {
            conn.close();
            conn = null;
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.storage.Store#status()
     */
    public TungstenProperties status()
    {
        // Get super class properties.
        TungstenProperties props = super.status();

        // Add properties for prefetch.
        props.setString("url", url);
        props.setString("slaveCatalogSchema", slaveCatalogSchema);
        props.setLong("interval", interval);
        props.setLong("maxTimeAhead", maxTimeAhead);
        props.setLong("minTimeAhead", minTimeAhead);
        props.setLong("sleepTime", sleepTime);
        props.setBoolean("allowAll", allowAll);

        // Add runtime properties.
        props.setLong("prefetchEvents", prefetchEvents);
        props.setLong("skippedEvents", totalEvents - prefetchEvents);
        double prefetchRatio = 0.0;
        if (totalEvents > 0)
            prefetchRatio = ((double) prefetchEvents) / totalEvents;
        props.setString("prefetchRatio", formatDouble(prefetchRatio));
        props.setString("prefetchState", prefetchState.toString());
        props.setString("slaveLatency", formatDouble(slaveLatency / 1000.0));
        props.setString("prefetchLatency",
                formatDouble(prefetchLatency / 1000.0));
        props.setString("prefetchTimeAhead",
                formatDouble((slaveLatency - prefetchLatency) / 1000.0));
        props.setString("prefetchState", prefetchState.toString());

        long duration = System.currentTimeMillis() - startTimeMillis;
        double timeActive = (duration - sleepTimeMillis) / 1000.0;
        double timeSleeping = sleepTimeMillis / 1000.0;
        props.setString("timeActive", formatDouble(timeActive));
        props.setString("timeSleeping", formatDouble(timeSleeping));

        return props;
    }

    // Format double values to 3 decimal places.
    private String formatDouble(double d)
    {
        return String.format("%-15.3f", d);
    }

    /**
     * Filter the event if it has already been executed.
     */
    public ReplDBMSEvent filter(ReplDBMSEvent event)
            throws ReplicatorException, InterruptedException
    {
        // Force a lock to ensure stats are shared.
        totalEvents++;

        // Get the time and latency of the event.
        Timestamp sourceTstamp = event.getExtractedTstamp();
        long currentTime = System.currentTimeMillis();
        prefetchLatency = currentTime - sourceTstamp.getTime();
        if (logger.isDebugEnabled())
        {
            logger.debug("Processing event: seqno=" + event.getSeqno()
                    + " prefetchLatency=" + prefetchLatency);
        }

        // Check latency of slave if we have not done so for a while.
        if (lastChecked == 0 || (currentTime - lastChecked >= interval))
        {
            getCurrentSlaveHeader();
        }

        // If we are debugging, stop right here. (If we exited earlier
        // the prefetch store stats would not be updated, which is very
        // confusing to outside observers.)
        if (allowAll)
        {
            prefetchEvents++;
            return event;
        }

        // Drop the event if the slave has already executed it.
        if (event.getSeqno() <= slaveSeqno)
        {
            if (logger.isDebugEnabled())
            {
                logger.debug("Discarding event already executed by slave seqno="
                        + event.getSeqno() + " slaveSeqno=" + slaveSeqno);
            }
            return null;
        }

        // If the event latency is less than the prescribed minimum latency
        // from the slave, drop it. This way we don't execute queries the slave
        // is about to do anyway.
        if ((slaveLatency - prefetchLatency) < aheadMinMillis)
        {
            if (logger.isDebugEnabled())
            {
                logger.debug("Discarding event that is under minimum prefetch latency: seqno="
                        + event.getSeqno()
                        + " prefetchLatency="
                        + prefetchLatency + " slaveLatency=" + slaveLatency);
            }
            return null;
        }

        // If our latency is too far below the slave, we need to wait.
        long aheadMillis = 0;
        long originalSlaveLatency = slaveLatency;
        while ((aheadMillis = originalSlaveLatency - prefetchLatency) > aheadMaxMillis)
        {
            // This event is too far ahead of the CommitSeqnoTable position:
            // Sleep some time and continue.
            if (logger.isDebugEnabled())
                logger.debug("Event is too far ahead of current slave position: aheadMillis="
                        + aheadMillis);
            long sleepStartMillis = System.currentTimeMillis();
            try
            {
                // Interrupted exception is passed up chain.
                prefetchState = PrefetchState.sleeping;
                Thread.sleep(sleepTime);
            }
            finally
            {
                prefetchState = PrefetchState.active;
                sleepTimeMillis += (System.currentTimeMillis() - sleepStartMillis);
            }

            // Recompute our latency, which increases as we sleep.
            currentTime = System.currentTimeMillis();
            prefetchLatency = currentTime - sourceTstamp.getTime();

            // Check slave's commit_seqno_table again to see which seqno it's
            // on.
            getCurrentSlaveHeader();

            // Drop the event if the slave has already executed it. This gets
            // out
            // of a trap if the slave suddenly jumps ahead while we are pausing.
            if (event.getSeqno() <= slaveSeqno)
            {
                if (logger.isDebugEnabled())
                {
                    logger.debug("Discarding event already executed by slave seqno="
                            + event.getSeqno() + " slaveSeqno=" + slaveSeqno);
                }
                return null;
            }
        }

        // Whatever we have now is ripe for fetching, so return it.
        prefetchEvents++;
        if (logger.isDebugEnabled() && totalEvents % 20000 == 0)
            logger.debug("Prefetched " + prefetchEvents + " events - Ratio "
                    + (100 * prefetchEvents / totalEvents) + "%");
        return event;
    }

    // Fetch position data from slave.
    private ReplDBMSHeaderData getCurrentSlaveHeader()
    {
        ReplDBMSHeaderData header = null;
        ResultSet rs = null;
        try
        {
            rs = seqnoStatement.executeQuery();
            if (rs.next())
            {
                // Construct header data
                long seqno = rs.getLong("seqno");
                short fragno = rs.getShort("fragno");
                boolean lastFrag = rs.getBoolean("last_frag");
                String sourceId = rs.getString("source_id");
                long epochNumber = rs.getLong("epoch_number");
                String eventId = rs.getString("eventid");
                // Record current slave latency and time of last check.
                slaveLatency = rs.getLong("applied_latency") * 1000;                

                header = new ReplDBMSHeaderData(seqno, fragno, lastFrag,
                        sourceId, epochNumber, eventId, null, new Timestamp(0), slaveLatency);


                slaveSeqno = header.getSeqno();
                lastChecked = System.currentTimeMillis();
            }
        }
        catch (SQLException e)
        {
            logger.warn(e);
        }
        finally
        {
            if (rs != null)
                try
                {
                    rs.close();
                }
                catch (SQLException e)
                {
                }
        }
        return header;
    }
}
