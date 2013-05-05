/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2012 Continuent Inc.
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
 * Initial developer(s): Teemu Ollakka, Robert Hodges
 * Contributor(s): Stephane Giron
 */

package com.continuent.tungsten.replicator.thl;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.cluster.resource.physical.Replicator;
import com.continuent.tungsten.common.config.Interval;
import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntime;
import com.continuent.tungsten.replicator.event.ReplControlEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.event.ReplEvent;
import com.continuent.tungsten.replicator.plugin.PluginContext;
import com.continuent.tungsten.replicator.storage.Store;
import com.continuent.tungsten.replicator.thl.log.DiskLog;
import com.continuent.tungsten.replicator.thl.log.LogConnection;
import com.continuent.tungsten.replicator.thl.serializer.ProtobufSerializer;
import com.continuent.tungsten.replicator.util.AtomicCounter;

/**
 * Implements a standard Store interface on the THL (transaction history log).
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class THL implements Store
{
    protected static Logger    logger               = Logger.getLogger(THL.class);

    // Version information and constants.
    public static final int    MAJOR                = 1;
    public static final int    MINOR                = 3;
    public static final String SUFFIX               = "";
    public static final String URI_SCHEME           = "thl";

    // Name of this store.
    private String             name;

    /** URL of storage listener. Default listens on all interfaces. */
    private String             storageListenerUri   = "thl://0.0.0.0:2112/";

    private String             logDir               = "/opt/continuent/logs/";
    private String             eventSerializer      = ProtobufSerializer.class
                                                            .getName();

    // Settable properties to control this storage implementation.
    protected String           password;
    protected String           url;
    protected String           user;
    protected String           vendor               = null;

    /** Number of events between resets on stream. */
    private int                resetPeriod          = 1;

    /** Store and compare checksum values on the log. */
    private boolean            doChecksum           = true;

    /** Name of the class used to serialize events. */
    protected String           eventSerializerClass = ProtobufSerializer.class
                                                            .getName();

    /** Log file maximum size in bytes. */
    protected int              logFileSize          = 1000000000;

    /** Log file retention in milliseconds. Defaults to 0 (= forever) */
    protected long             logFileRetainMillis  = 0;

    /** Idle log Connection timeout in seconds. */
    protected int              logConnectionTimeout = 28800;

    /** I/O buffer size in bytes. */
    protected int              bufferSize           = 131072;

    /**
     * Flush data after this many milliseconds. 0 flushes after every write.
     */
    private long               flushIntervalMillis  = 0;

    /** If true, fsync when flushing. */
    private boolean            fsyncOnFlush         = false;

    // Database storage and disk log.
    private CatalogManager     catalog              = null;
    private DiskLog            diskLog              = null;

    // Storage management variables.
    protected PluginContext    context;
    private AtomicCounter      sequencer;

    // Storage connectivity.
    private Server             server               = null;

    private boolean            readOnly             = false;

    // This indicates whether replicator will stop or keep on trying to extract
    // data despite errors while storing its position into database
    // (CommitSeqno)
    private boolean            stopOnDBError        = true;

    /** Creates a store instance. */
    public THL()
    {
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    // Accessors for configuration.
    public String getStorageListenerUri()
    {
        return storageListenerUri;
    }

    public void setStorageListenerUri(String storageListenerUri)
    {
        this.storageListenerUri = storageListenerUri;
    }

    public String getUrl()
    {
        return url;
    }

    public void setUrl(String url)
    {
        this.url = url;
    }

    public void setVendor(String vendor)
    {
        this.vendor = vendor;
    }

    public String getUser()
    {
        return user;
    }

    public void setUser(String user)
    {
        this.user = user;
    }

    public String getPassword()
    {
        return password;
    }

    public void setPassword(String password)
    {
        this.password = password;
    }

    public int getResetPeriod()
    {
        return resetPeriod;
    }

    public void setResetPeriod(int resetPeriod)
    {
        this.resetPeriod = resetPeriod;
    }

    /**
     * Sets the logDir value.
     * 
     * @param logDir The logDir to set.
     */
    public void setLogDir(String logDir)
    {
        this.logDir = logDir;
    }

    /**
     * Sets the logFileSize value in bytes.
     * 
     * @param logFileSize The logFileSize to set.
     */
    public void setLogFileSize(int logFileSize)
    {
        this.logFileSize = logFileSize;
    }

    /**
     * Determines whether to checksum log records.
     * 
     * @param doChecksum If true use checksums
     */
    public void setDoChecksum(boolean doChecksum)
    {
        this.doChecksum = doChecksum;
    }

    /**
     * Sets the event serializer name.
     */
    public void setEventSerializer(String eventSerializer)
    {
        this.eventSerializer = eventSerializer;
    }

    /**
     * Sets the log file retention interval.
     */
    public void setLogFileRetention(String logFileRetention)
    {
        this.logFileRetainMillis = new Interval(logFileRetention).longValue();
    }

    /**
     * Sets the idle log connection timeout in seconds.
     */
    public void setLogConnectionTimeout(int logConnectionTimeout)
    {
        this.logConnectionTimeout = logConnectionTimeout;
    }

    /**
     * Sets the log buffer size.
     */
    public void setBufferSize(int bufferSize)
    {
        this.bufferSize = bufferSize;
    }

    /**
     * Sets the interval between flush calls.
     */
    public void setFlushIntervalMillis(long flushIntervalMillis)
    {
        this.flushIntervalMillis = flushIntervalMillis;
    }

    /**
     * If set to true, perform an fsync with every flush. Warning: fsync is very
     * slow, so you want a long flush interval in this case.
     */
    public synchronized void setFsyncOnFlush(boolean fsyncOnFlush)
    {
        this.fsyncOnFlush = fsyncOnFlush;
    }

    public void setReadOnly(String ro)
    {
        readOnly = (ro.equals("true"));
    }

    public void setStopOnDBError(boolean stopOnDBErr)
    {
        this.stopOnDBError = stopOnDBErr;
    }

    public boolean getStopOnDBError()
    {
        return stopOnDBError;
    }

    // STORE API STARTS HERE.

    /**
     * Return max stored sequence number.
     */
    public long getMaxStoredSeqno()
    {
        // This prevents race conditions when going offline.
        DiskLog localCopy = diskLog;
        if (localCopy == null)
            return -1;
        else
            return localCopy.getMaxSeqno();
    }

    /**
     * Return minimum stored sequence number.
     */
    public long getMinStoredSeqno()
    {
        // This prevents race conditions when going offline.
        DiskLog localCopy = diskLog;
        if (localCopy == null)
            return -1;
        else
            return localCopy.getMinSeqno();
    }

    /**
     * Updates the active sequence number on the log. Log files can only be
     * deleted if their last sequence number is below this value.
     */
    public void updateActiveSeqno(long activeSeqno)
    {
        diskLog.setActiveSeqno(activeSeqno);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        // Store variables.
        this.context = context;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public synchronized void prepare(PluginContext context)
            throws ReplicatorException, InterruptedException
    {
        // Prepare database connection.
        if (url != null && url.trim().length() > 0)
        {
            logger.info("Preparing SQL catalog tables");
            ReplicatorRuntime runtime = (ReplicatorRuntime) context;
            String metadataSchema = context.getReplicatorSchemaName();
            catalog = new CatalogManager(runtime);
            catalog.connect(url, user, password, metadataSchema, vendor);
            catalog.prepareSchema(context);
        }
        else
            logger.info("SQL catalog tables are disabled");

        // Configure and prepare the log.
        diskLog = new DiskLog();
        diskLog.setDoChecksum(doChecksum);
        diskLog.setEventSerializerClass(eventSerializer);
        diskLog.setLogDir(logDir);
        diskLog.setLogFileSize(logFileSize);
        diskLog.setLogFileRetainMillis(logFileRetainMillis);
        diskLog.setLogConnectionTimeoutMillis(logConnectionTimeout * 1000);
        diskLog.setBufferSize(bufferSize);
        diskLog.setFsyncOnFlush(fsyncOnFlush);
        if (fsyncOnFlush)
        {
            // Only used with fsync.
            diskLog.setFlushIntervalMillis(flushIntervalMillis);
        }
        diskLog.setReadOnly(readOnly);
        diskLog.prepare();
        logger.info("Log preparation is complete");

        // Start server for THL connections.
        if (context.isRemoteService() == false)
        {
            try
            {
                server = new Server(context, sequencer, this);
                server.start();
            }
            catch (IOException e)
            {
                throw new ReplicatorException("Unable to start THL server", e);
            }
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public synchronized void release(PluginContext context)
            throws InterruptedException, ReplicatorException
    {
        // Cancel server.
        if (server != null)
        {
            try
            {
                server.stop();
            }
            catch (InterruptedException e)
            {
                logger.warn(
                        "Server stop operation was unexpectedly interrupted", e);
            }
            finally
            {
                server = null;
            }
        }

        if (catalog != null)
        {
            catalog.close(context);
            catalog = null;
        }
        if (diskLog != null)
        {
            diskLog.release();
            diskLog = null;
        }
    }

    /**
     * Connect to the log. Adapters must call this to use the log.
     * 
     * @param readonly If true, this is a readonly connection
     * @return A disk log client
     */
    public LogConnection connect(boolean readonly) throws ReplicatorException
    {
        return diskLog.connect(readonly);
    }

    /**
     * Disconnect from the log. Adapters must call this to free resources and
     * avoid leaks.
     * 
     * @param client a Disk log client to be disconnected
     * @throws ReplicatorException
     */
    public void disconnect(LogConnection client) throws ReplicatorException
    {
        client.release();
    }

    /**
     * Updates the sequence number stored in the catalog trep_commit_seqno. If
     * the catalog is disabled we do nothing, which allows us to run unit tests
     * easily without a DBMS present.
     * 
     * @throws ReplicatorException Thrown if update is unsuccessful
     */
    public void updateCommitSeqno(THLEvent thlEvent) throws ReplicatorException
    {
        if (catalog == null)
        {
            if (logger.isDebugEnabled())
                logger.debug("Seqno update is disabled: seqno="
                        + thlEvent.getSeqno());
        }
        else
        {
            try
            {
                catalog.updateCommitSeqnoTable(thlEvent);
            }
            catch (SQLException e)
            {
                throw new THLException(
                        "Unable to update commit sequence number: seqno="
                                + thlEvent.getSeqno() + " event id="
                                + thlEvent.getEventId(), e);
            }
        }
    }

    /**
     * Returns true if the indicated sequence number is available.
     */
    public boolean pollSeqno(long seqno)
    {
        return seqno <= diskLog.getMaxSeqno();
    }

    /**
     * Get the last applied event. We first try the disk log then if that is
     * absent try the catalog. If there is nothing there we must be starting
     * from scratch and return null.
     * 
     * @return An event header or null if log is newly initialized
     * @throws InterruptedException
     * @throws ReplicatorException
     */
    public ReplDBMSHeader getLastAppliedEvent() throws ReplicatorException,
            InterruptedException
    {
        // Look for maximum sequence number in log and use that if available.
        if (diskLog != null)

        {
            long maxSeqno = diskLog.getMaxSeqno();
            if (maxSeqno > -1)
            {
                LogConnection conn = null;
                try
                {
                    // Try to connect and find the event.
                    THLEvent thlEvent = null;
                    conn = diskLog.connect(true);
                    conn.seek(maxSeqno);
                    while ((thlEvent = conn.next(false)) != null
                            && thlEvent.getSeqno() == maxSeqno)
                    {
                        // Return only the last fragment.
                        if (thlEvent.getLastFrag())
                        {
                            ReplEvent event = thlEvent.getReplEvent();
                            if (event instanceof ReplDBMSEvent)
                                return (ReplDBMSEvent) event;
                            else if (event instanceof ReplControlEvent)
                                return ((ReplControlEvent) event).getHeader();
                        }
                    }

                    // If we did not find the last fragment of the event
                    // we need to warn somebody.
                    if (thlEvent != null)
                        logger.warn("Unable to find last fragment of event: seqno="
                                + maxSeqno);
                }
                finally
                {
                    conn.release();
                }
            }
        }

        // If that does not work, try the catalog.
        if (catalog != null)
        {
            return catalog.getMinLastEvent();
        }

        // If we get to this point, the log is newly initialized and there is no
        // such event to return.
        return null;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.storage.Store#status()
     */
    @Override
    public synchronized TungstenProperties status()
    {
        TungstenProperties props = new TungstenProperties();
        props.setLong(Replicator.MIN_STORED_SEQNO, getMinStoredSeqno());
        props.setLong(Replicator.MAX_STORED_SEQNO, getMaxStoredSeqno());
        props.setLong("activeSeqno", diskLog.getActiveSeqno());
        props.setBoolean("doChecksum", doChecksum);
        props.setString("logDir", logDir);
        props.setInt("logFileSize", logFileSize);
        props.setLong("logFileRetainMillis", logFileRetainMillis);
        props.setLong("logFileSize", diskLog.getLogFileSize());
        props.setLong("timeoutMillis", diskLog.getTimeoutMillis());
        props.setBoolean("fsyncOnFlush", fsyncOnFlush);
        props.setLong("flushIntervalMillis", diskLog.getFlushIntervalMillis());
        props.setLong("timeoutMillis", diskLog.getTimeoutMillis());
        props.setLong("logConnectionTimeout", logConnectionTimeout);
        props.setBoolean("readOnly", readOnly);

        return props;
    }
}