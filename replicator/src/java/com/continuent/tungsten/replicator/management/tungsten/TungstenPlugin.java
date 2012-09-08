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
 * Initial developer(s): Seppo Jaakola
 * Contributor(s): Teemu Ollakka, Robert Hodges, Alex Yurchenko, Linas Virbalas,
 * Stephane Giron
 */

package com.continuent.tungsten.replicator.management.tungsten;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

import javax.management.NotificationBroadcasterSupport;

import org.apache.log4j.Logger;

import com.continuent.tungsten.commons.cluster.resource.OpenReplicatorParams;
import com.continuent.tungsten.commons.cluster.resource.physical.Replicator;
import com.continuent.tungsten.commons.cluster.resource.physical.ReplicatorCapabilities;
import com.continuent.tungsten.commons.config.TungstenProperties;
import com.continuent.tungsten.commons.config.WildcardPattern;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.channel.ChannelAssignmentService;
import com.continuent.tungsten.replicator.conf.ReplicatorConf;
import com.continuent.tungsten.replicator.conf.ReplicatorMonitor;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntime;
import com.continuent.tungsten.replicator.consistency.ConsistencyCheck;
import com.continuent.tungsten.replicator.consistency.ConsistencyCheckFactory;
import com.continuent.tungsten.replicator.consistency.ConsistencyTable;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.DatabaseFactory;
import com.continuent.tungsten.replicator.database.Table;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.extractor.Extractor;
import com.continuent.tungsten.replicator.extractor.ExtractorException;
import com.continuent.tungsten.replicator.heartbeat.HeartbeatTable;
import com.continuent.tungsten.replicator.management.OpenReplicatorContext;
import com.continuent.tungsten.replicator.management.OpenReplicatorPlugin;
import com.continuent.tungsten.replicator.management.events.GoOfflineEvent;
import com.continuent.tungsten.replicator.management.events.OfflineNotification;
import com.continuent.tungsten.replicator.pipeline.Pipeline;
import com.continuent.tungsten.replicator.pipeline.ShardProgress;
import com.continuent.tungsten.replicator.pipeline.Stage;
import com.continuent.tungsten.replicator.pipeline.StageProgressTracker;
import com.continuent.tungsten.replicator.pipeline.TaskProgress;
import com.continuent.tungsten.replicator.plugin.PluginSpecification;
import com.continuent.tungsten.replicator.service.PipelineService;
import com.continuent.tungsten.replicator.shard.ShardManager;
import com.continuent.tungsten.replicator.storage.Store;
import com.continuent.tungsten.replicator.util.Watch;
import com.continuent.tungsten.replicator.util.WatchAction;

/**
 * This class defines a ReplicatorManager, which is the starting class for a
 * Tungsten Replicator instance. The ReplicatorManager accepts the following
 * Java properties which may be set prior to startup.
 * 
 * @author <a href="mailto:seppo.jaakola@continuent.com">Seppo Jaakola</a>
 * @version 1.0
 */
public class TungstenPlugin extends NotificationBroadcasterSupport
        implements
            OpenReplicatorPlugin
{
    private static Logger         logger     = Logger.getLogger(TungstenPlugin.class);

    // Configuration is stored in the ReplicatorRuntime
    private TungstenProperties    properties = null;
    private ReplicatorRuntime     runtime;
    private Pipeline              pipeline;
    private OpenReplicatorContext context;
    private ShardManager          shardManager;

    /**
     * Set event dispatcher and instantiate the Tungsten monitor. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.management.OpenReplicatorPlugin#prepare(OpenReplicatorContext)
     */
    public void prepare(OpenReplicatorContext context)
            throws ReplicatorException
    {
        this.context = context;

        // Instantiate monitoring data for Tungsten Replicator.
        @SuppressWarnings("unused")
        ReplicatorMonitor monitor = ReplicatorMonitor.getInstance();

        // KLUDGE: Do we really need this now?
        // JmxManager.registerMBean(monitor, ReplicatorMonitor.class);
    }

    /**
     * Deallocate all resources.
     * 
     * @see com.continuent.tungsten.replicator.management.OpenReplicatorPlugin#release()
     */
    public void release() throws ReplicatorException
    {
        try
        {
            doShutdown(new TungstenProperties());
            properties = null;
        }
        catch (Throwable e)
        {
            logger.error(
                    "Replicator service shutdown failed due to underlying error: ",
                    e);
            throw new ReplicatorException(
                    "Replicator service shutdown failed due to underlying error: "
                            + e);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.management.OpenReplicatorPlugin#consistencyCheck(java.lang.String,
     *      java.lang.String, java.lang.String, int, int)
     */
    public void consistencyCheck(String method, String schemaName,
            String tableName, int rowOffset, int rowLimit) throws Exception
    {
        logger.info("Got consistency check request: " + method + ":"
                + schemaName + "." + tableName + ":" + rowOffset + ","
                + rowLimit);

        // Ensure we have a runtime. This is null if we are offline.
        // TODO: This command should execute in a state machine.
        ReplicatorRuntime ourRuntime = runtime;
        if (ourRuntime == null)
        {
            logger.warn("Master is offline and cannot perform consistency check");
            throw new Exception(
                    "Unable to obtain runtime for consistency check; is replicator offline?");
        }
        String url = ourRuntime.getJdbcUrl(null);
        String user = ourRuntime.getJdbcUser();
        String password = ourRuntime.getJdbcPassword();

        Database conn = null;

        try
        {
            try
            // connect to database
            {
                conn = DatabaseFactory.createDatabase(url, user, password);
                // this is about the only place where we want logging the
                // queries
                conn.connect(true);
            }
            catch (Exception e)
            {
                logger.error("Failed to connect to database server: "
                        + e.getMessage());
                throw e;
            }

            // get a list of tables to check
            ArrayList<Table> tables;
            try
            {
                if (tableName == null)
                {
                    tables = conn.getTables(schemaName, true);
                    rowOffset = ConsistencyTable.ROW_UNSET;
                    rowLimit = ConsistencyTable.ROW_UNSET;
                }
                else
                {
                    tables = new ArrayList<Table>();
                    if (!tableName.contains("*") && !tableName.contains("?"))
                    {
                        // Simple table name match requested.
                        Table checkOneTable = conn.findTable(schemaName,
                                tableName);
                        if (checkOneTable != null)
                            tables.add(checkOneTable);
                    }
                    else
                    {
                        // Wildcard table name match requested.
                        ArrayList<Table> allTables = conn.getTables(schemaName,
                                true);
                        for (Table table : allTables)
                        {
                            if (Pattern.matches(
                                    WildcardPattern.wildcardToRegex(tableName),
                                    table.getName()))
                                tables.add(table);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                logger.error("Failed to query database for tables: "
                        + e.getMessage());
                throw e;
            }

            // Find the last consistency check id
            Table ct = null;
            int id = 1;
            try
            {
                Statement st;
                st = conn.createStatement();
                ct = conn.findTable(properties.getString("replicator.schema"),
                        ConsistencyTable.TABLE_NAME);
                ResultSet rs = st.executeQuery("SELECT MAX("
                        + ConsistencyTable.idColumnName + ") FROM "
                        + ct.getSchema() + "." + ct.getName());
                if (rs.next())
                {
                    id = rs.getInt(1) + 1; // increment id
                }
                rs.close();
                st.close();
            }
            catch (Exception e)
            {
                logger.error("Failed to query last consistency check ID: "
                        + e.getMessage());
                throw e;
            }

            for (int i = 0; i < tables.size(); i++)
            {
                ConsistencyCheck cc = ConsistencyCheckFactory
                        .createConsistencyCheck(id, tables.get(i), rowOffset,
                                rowLimit, method,
                                ourRuntime.isConsistencyCheckColumnNames(),
                                ourRuntime.isConsistencyCheckColumnTypes());

                try
                {
                    conn.consistencyCheck(ct, cc);
                }
                catch (Exception e)
                {
                    logger.error("Consistency check transaction ("
                            + cc.toString() + ") failed: " + e.getMessage());
                    throw e;
                }
            }

        }
        finally
        {
            conn.close();
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.management.OpenReplicatorPlugin#configure(com.continuent.tungsten.commons.config.TungstenProperties)
     */
    public void configure(TungstenProperties properties)
            throws ReplicatorException
    {
        // Remember our properties
        this.properties = properties;

        // Release existing runtime, if any, and generate a new one.
        doCreateRuntime(null);

        // Start the shard manager if it is not already started. We must
        // have a valid runtime at this point so it is safe to use it.
        {
            // Start the shard manager.
            if (shardManager == null)
                try
                {
                    shardManager = new ShardManager(runtime.getServiceName(),
                            runtime.getJdbcUrl(null), runtime.getJdbcUser(),
                            runtime.getJdbcPassword(),
                            runtime.getReplicatorSchemaName(),
                            runtime.getTungstenTableType());
                    shardManager.advertiseInternal();
                }
                catch (Exception e)
                {
                    throw new ReplicatorException(String.format(
                            "Unable to instantiate shard manager service '%s'",
                            runtime.getServiceName()), e);
                }

        }
    }

    /**
     * Puts the replicator into the online state, which turns on replication.
     */
    public void online(TungstenProperties params) throws Exception
    {
        // Release existing runtime, if any, and generate a new one.
        doCreateRuntime(params);

        // Start replication pipeline.
        try
        {
            logger.info("Starting replication service: role="
                    + runtime.getRoleName());
            runtime.prepare();
            pipeline = runtime.getPipeline();

            // Set initial event ID if specified.
            String initialEventId = params
                    .getString(OpenReplicatorParams.INIT_EVENT_ID);
            if (initialEventId != null)
            {
                logger.info("Initializing extractor to start at specific event ID: "
                        + initialEventId);
                pipeline.setInitialEventId(initialEventId);
            }

            // Set apply skip events, if specified and higher than 0.
            if (params.getString(OpenReplicatorParams.SKIP_APPLY_EVENTS) != null)
            {
                try
                {
                    long skipCount = params
                            .getLong(OpenReplicatorParams.SKIP_APPLY_EVENTS);
                    if (skipCount < 0)
                        throw new ReplicatorException(
                                "Apply skip count may not be less than 0: "
                                        + skipCount);
                    else
                        pipeline.setApplySkipCount(skipCount);
                }
                catch (NumberFormatException e)
                {
                    throw new ReplicatorException(
                            "Invalid apply skip count: "
                                    + params.getString(OpenReplicatorParams.SKIP_APPLY_EVENTS));
                }
            }

            // Set apply skip seqnos.
            if (params.getString(OpenReplicatorParams.SKIP_APPLY_SEQNOS) != null)
            {
                try
                {
                    String seqnosToBeSkipped = params
                            .getString(OpenReplicatorParams.SKIP_APPLY_SEQNOS);
                    SortedSet<Long> seqnos = new TreeSet<Long>();

                    String[] seqnoRanges = seqnosToBeSkipped.split(",");
                    for (String seqnoRange : seqnoRanges)
                    {
                        String[] seqnoBoundaries = seqnoRange.trim().split("-");
                        if (seqnoBoundaries.length == 1)
                        {
                            seqnos.add(Long.parseLong(seqnoBoundaries[0].trim()));
                        }
                        else if (seqnoBoundaries.length == 2)
                        {
                            Long start = Long.parseLong(seqnoBoundaries[0]
                                    .trim());
                            Long end = Long
                                    .parseLong(seqnoBoundaries[1].trim());
                            if (start < end)
                            {
                                for (Long i = start; i <= end; i++)
                                {
                                    seqnos.add(i);
                                }
                            }
                            else
                            {
                                throw new ReplicatorException(
                                        "Invalid apply skip seqnos: "
                                                + params.getString(OpenReplicatorParams.SKIP_APPLY_SEQNOS));
                            }
                        }
                        else
                        {
                            throw new ReplicatorException(
                                    "Invalid apply skip seqnos: "
                                            + params.getString(OpenReplicatorParams.SKIP_APPLY_SEQNOS));
                        }
                    }
                    logger.info("Going online and skipping events " + seqnos);
                    pipeline.setApplySkipEvents(seqnos);
                }
                catch (NumberFormatException e)
                {
                    throw new ReplicatorException(
                            "Invalid apply skip seqnos: "
                                    + params.getString(OpenReplicatorParams.SKIP_APPLY_SEQNOS));
                }
            }

            // Stay online to a specified sequence number.
            if (params.get(OpenReplicatorParams.ONLINE_TO_SEQNO) != null)
            {
                long seqno = params
                        .getLong(OpenReplicatorParams.ONLINE_TO_SEQNO);
                logger.info("Initializing pipeline to go offline after processing seqno: "
                        + seqno);
                pipeline.shutdownAfterSequenceNumber(seqno);
            }

            // Stay online to a specified event ID.
            if (params.get(OpenReplicatorParams.ONLINE_TO_EVENT_ID) != null)
            {
                String eventId = params
                        .getString(OpenReplicatorParams.ONLINE_TO_EVENT_ID);
                logger.info("Initializing pipeline to go offline after processing event ID: "
                        + eventId);
                pipeline.shutdownAfterEventId(eventId);
            }

            // Stay online to a heartbeat event.
            if (params.get(OpenReplicatorParams.ONLINE_TO_HEARTBEAT) != null)
            {
                String name = params.getString(
                        OpenReplicatorParams.ONLINE_TO_HEARTBEAT, "*", true);
                logger.info("Initializing pipeline to go offline after processing hearbeat");
                pipeline.shutdownAfterHeartbeat(name);
            }

            // Stay online to a specified timestamp.
            if (params.get(OpenReplicatorParams.ONLINE_TO_TIMESTAMP) != null)
            {
                long timeMillis = params
                        .getLong(OpenReplicatorParams.ONLINE_TO_TIMESTAMP);
                DateFormat formatter = new SimpleDateFormat(
                        "yyyy-MM-dd HH:mm:ss");
                Date toDate = new Date(timeMillis);
                Timestamp ts = new Timestamp(timeMillis);
                logger.info("Scheduling pipeline to go offline after processing source timestamp: "
                        + formatter.format(toDate));
                pipeline.shutdownAfterTimestamp(ts);
            }

            // Masters have extra work.
            if (runtime.isMaster()
                    && runtime.getReplicatorProperties().getBoolean(
                            ReplicatorConf.MASTER_THL_CHECK,
                            ReplicatorConf.MASTER_THL_CHECK_DEFAULT, false))
            {
                // Check if this host was already the master previously :
                // Get the last applied event from the tail applier and compare
                // its source id to this host source id
                ReplDBMSHeader lastAppliedEvent = pipeline.getTailApplier()
                        .getLastEvent();
                if (lastAppliedEvent != null
                        && !lastAppliedEvent.getSourceId().equals(
                                runtime.getSourceId()))
                {
                    // Last applied event source id is different from this host
                    // source id, which means that this host was a slave
                    // previously.

                    // Check if THL is in sync with trep_commit_seqno before
                    // going online, as otherwise, we could loose some updates
                    long maxStoredSeqno = pipeline.getMaxStoredSeqno();
                    long maxCommittedSeqno = pipeline.getMaxCommittedSeqno();
                    if (maxStoredSeqno > maxCommittedSeqno)
                        throw new ReplicatorException("Database (@seqno "
                                + maxCommittedSeqno
                                + ") does not seem in sync with THL (@seqno "
                                + maxStoredSeqno
                                + "). The last stored event (#"
                                + maxStoredSeqno + ") was extracted by "
                                + lastAppliedEvent.getSourceId()
                                + " and was not applied on "
                                + runtime.getSourceId() + ".");
                }
            }

            // Start the pipeline.
            pipeline.start(context.getEventDispatcher());

            // Masters are extra work.
            if (runtime.isMaster())
            {
                // Put an initial event into the log. (Carry-over from
                // old replicator; solves TREP-324.
                logger.info("Adding heartbeat for master");
                TungstenProperties props = new TungstenProperties();
                props.setString(OpenReplicatorParams.HEARTBEAT_NAME,
                        "MASTER_ONLINE");
                heartbeat(props);
            }
        }
        catch (ReplicatorException e)
        {
            throw e;
        }
        catch (Throwable e)
        {
            String pendingError = "Unable to start replication service due to underlying error";
            logger.error(pendingError, e);
            throw new ReplicatorException(pendingError + ": " + e);
        }
    }

    /**
     * Puts the replicator immediately into the offline state, which turns off
     * replication. This operation is a hard shutdown that does no clean-up. If
     * clean-up is required, call deferredShutdown() instead.
     */
    public void offline(TungstenProperties params) throws Exception
    {
        try
        {
            // Now we go ahead and shutdown completely.
            doShutdown(params);
            context.getEventDispatcher().put(new OfflineNotification());
        }
        catch (ReplicatorException e)
        {
            String pendingError = "Replicator service shutdown failed";
            if (logger.isDebugEnabled())
                logger.debug(pendingError, e);
            throw e;
        }
        catch (Throwable e)
        {
            String pendingError = "Replicator service shutdown failed due to underlying error";
            logger.error(pendingError, e);
            throw new ReplicatorException(pendingError + e);
        }
    }

    /**
     * Performs a deferred shutdown. All deferred operations then enqueue a
     * GoOfflineEvent to do a hard shutdown.
     * 
     * @see com.continuent.tungsten.replicator.management.OpenReplicatorPlugin#offlineDeferred(com.continuent.tungsten.commons.config.TungstenProperties)
     */
    public void offlineDeferred(TungstenProperties params) throws Exception
    {
        try
        {
            if (params.get(OpenReplicatorParams.OFFLINE_TRANSACTIONAL) != null)
            {
                // Shut down processing in orderly fashion and signal that we
                // are down.
                logger.info("Initiating clean shutdown at next transaction");
                pipeline.shutdown(false);
                pipeline.getContext().getEventDispatcher()
                        .put(new GoOfflineEvent());
            }
            else if (params.get(OpenReplicatorParams.OFFLINE_AT_SEQNO) != null)
            {
                // Shut down processing at a particular sequence number.
                long seqno = params
                        .getLong(OpenReplicatorParams.OFFLINE_AT_SEQNO);
                logger.info("Initializing pipeline to go offline after processing seqno: "
                        + seqno);
                pipeline.shutdownAfterSequenceNumber(seqno);
            }
            else if (params.get(OpenReplicatorParams.OFFLINE_AT_EVENT_ID) != null)
            {
                // Shut down processing at a particular event ID.
                String eventId = params
                        .getString(OpenReplicatorParams.OFFLINE_AT_EVENT_ID);
                logger.info("Initializing pipeline to go offline after processing event ID: "
                        + eventId);
                pipeline.shutdownAfterEventId(eventId);
            }
            // Stay online to a heartbeat event.
            else if (params.get(OpenReplicatorParams.OFFLINE_AT_HEARTBEAT) != null)
            {
                String name = params.getString(
                        OpenReplicatorParams.OFFLINE_AT_HEARTBEAT, "*", true);
                logger.info("Scheduline pipeline to go offline after processing hearbeat");
                pipeline.shutdownAfterHeartbeat(name);
            }

            // Stay online to a specified timestamp.
            else if (params.get(OpenReplicatorParams.OFFLINE_AT_TIMESTAMP) != null)
            {
                long timeMillis = params
                        .getLong(OpenReplicatorParams.OFFLINE_AT_TIMESTAMP);
                DateFormat formatter = new SimpleDateFormat(
                        "yyyy-MM-dd HH:mm:ss");
                Date toDate = new Date(timeMillis);
                Timestamp ts = new Timestamp(timeMillis);
                logger.info("Scheduling pipeline to go offline after processing source timestamp: "
                        + formatter.format(toDate));
                pipeline.shutdownAfterTimestamp(ts);
            }

            // If there is no parameter provided, just enqueue an event to
            // perform a hard shutdown.
            else
            {
                logger.info("Initiating immediate pipeline shutdown");
                context.getEventDispatcher().put(new GoOfflineEvent());
            }
        }
        catch (ReplicatorException e)
        {
            String pendingError = "Replicator deferred service shutdown failed";
            if (logger.isDebugEnabled())
                logger.debug(pendingError, e);
            throw e;
        }
        catch (Throwable e)
        {
            String pendingError = "Replicator deferred service shutdown failed due to underlying error";
            logger.error(pendingError, e);
            throw new ReplicatorException(pendingError + e);
        }
    }

    /**
     * Starts a heartbeat event. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.management.OpenReplicatorPlugin#heartbeat(com.continuent.tungsten.commons.config.TungstenProperties)
     */
    public boolean heartbeat(TungstenProperties params) throws Exception
    {
        if (runtime == null)
            return false;

        else
        {
            String url = runtime.getJdbcUrl(null);
            String user = runtime.getJdbcUser();
            String password = runtime.getJdbcPassword();
            String name = params.getString(OpenReplicatorParams.HEARTBEAT_NAME,
                    "NONE", true);
            try
            {
                HeartbeatTable htTable = new HeartbeatTable(
                        runtime.getReplicatorSchemaName(),
                        runtime.getTungstenTableType());
                htTable.startHeartbeat(url, user, password, name);
                return true;
            }
            catch (SQLException e)
            {
                throw new ReplicatorException(
                        "Heartbeat operation failed due to SQL error: "
                                + e.getMessage(), e);
            }
        }
    }

    /**
     * Implements a flush operation to synchronize the state of the database
     * with the replication log and return a comparable event ID that can be
     * used in a wait operation on a slave.
     * 
     * @param timeout Number of seconds to wait. 0 is indefinite.
     * @return The event ID at which the log is synchronized
     */
    public String flush(long timeout) throws Exception
    {
        // First insert a heartbeat to ensure there is something in the
        // that we can wait for.
        TungstenProperties props = new TungstenProperties();
        props.setString(OpenReplicatorParams.HEARTBEAT_NAME, "FLUSH");
        heartbeat(props);

        // Wait for the event we were seeking to show up.
        Future<ReplDBMSHeader> expectedEvent = runtime.getPipeline().flush();
        ReplDBMSHeader event = null;
        try
        {
            if (timeout <= 0)
                event = expectedEvent.get();
            else
                event = expectedEvent.get(timeout, TimeUnit.SECONDS);
        }
        finally
        {
            expectedEvent.cancel(false);
        }

        long seqno = event.getSeqno();
        logger.info("SyncEvent-Flush: Flush complete.  Returning sequence number: "
                + seqno);
        return new Long(seqno).toString();
    }

    /**
     * Wait for a particular event to be applied on the slave.
     * 
     * @param event Event to wait for
     * @param timeout Number of seconds to wait. 0 is indefinite.
     * @return true if requested sequence number or greater applied, else false
     *         if the wait timed out
     * @throws Exception if there is a timeout or we are canceled
     */
    public boolean waitForAppliedEvent(String event, long timeout)
            throws Exception
    {
        // The event returns a Future on the THLEvent we are
        // expecting. We just wait for it.
        long seqno = new Long(event);

        if (pipeline == null)
            throw new ReplicatorException(
                    "Invalid replicator state for this operation. Cannot wait for event "
                            + seqno + " to be applied.");

        Future<ReplDBMSHeader> expectedEvent = pipeline
                .watchForCommittedSequenceNumber(seqno, false);

        ReplDBMSHeader replEvent = null;
        try
        {
            if (timeout <= 0)
                replEvent = expectedEvent.get();
            else
                replEvent = expectedEvent.get(timeout, TimeUnit.SECONDS);
            logger.info("SyncEvent-WaitSeqno: Sequence number " + seqno
                    + " found or surpassed with sequence number: "
                    + replEvent.getSeqno());
        }
        catch (TimeoutException e)
        {
            return false;
        }
        finally
        {
            expectedEvent.cancel(true);
        }
        // If we got here it worked.
        return true;
    }

    /**
     * Returns the current replicator status as a set of name-value pairs.
     */
    public HashMap<String, String> status() throws Exception
    {
        TungstenProperties statusProps = new TungstenProperties();

        // Set generic values that are always identical.
        statusProps
                .setString(Replicator.SEQNO_TYPE, Replicator.SEQNO_TYPE_LONG);

        // Set default values.
        statusProps.setLong(Replicator.APPLIED_LAST_SEQNO, -1);
        statusProps.setString(Replicator.APPLIED_LAST_EVENT_ID, "NONE");
        statusProps.setLong(Replicator.MIN_STORED_SEQNO, -1);
        statusProps.setLong(Replicator.MAX_STORED_SEQNO, -1);
        statusProps.setLong(Replicator.LATEST_EPOCH_NUMBER, -1);
        statusProps.setDouble(Replicator.APPLIED_LATENCY, -1.0);
        statusProps.setString(Replicator.CURRENT_EVENT_ID, "NONE");
        statusProps.setString(Replicator.OFFLINE_REQUESTS, "NONE");

        // The following logic avoids race conditions that may cause
        // different sources of information to be null.
        Pipeline pipeline = null;
        List<String> extensions = null;
        String type = "unknown";
        if (runtime != null)
        {
            pipeline = runtime.getPipeline();
            extensions = runtime.getExtensionNames();
            type = (runtime.isRemoteService() ? "remote" : "local");
        }
        ReplDBMSHeader lastEvent = null;
        if (pipeline != null)
        {
            lastEvent = pipeline.getLastAppliedEvent();

            // The current event ID may be null for slaves or non-DBMS
            // sources.
            Extractor headExtractor = pipeline.getHeadExtractor();
            String currentEventId = null;
            if (headExtractor != null)
            {
                try
                {
                    currentEventId = headExtractor.getCurrentResourceEventId();
                    if (currentEventId != null)
                        statusProps.setString(Replicator.CURRENT_EVENT_ID,
                                currentEventId);
                }
                catch (ExtractorException e)
                {
                    statusProps.setString(Replicator.CURRENT_EVENT_ID, "ERROR");
                    if (logger.isDebugEnabled())
                        logger.debug("Unable to get current resource event ID",
                                e);
                }
            }

            // Get the current list of offline requests.
            String offlineRequests = pipeline.getOfflineRequests();
            if (offlineRequests.length() > 0)
                statusProps.setString(Replicator.OFFLINE_REQUESTS,
                        offlineRequests);

            // Show event processing information.
            if (lastEvent != null)
            {
                statusProps.setLong(Replicator.APPLIED_LAST_SEQNO,
                        lastEvent.getSeqno());
                statusProps.setLong(Replicator.LATEST_EPOCH_NUMBER,
                        lastEvent.getEpochNumber());
                statusProps.setString(Replicator.APPLIED_LAST_EVENT_ID,
                        lastEvent.getEventId());
                statusProps.setLong(Replicator.MIN_STORED_SEQNO,
                        pipeline.getMinStoredSeqno());
                statusProps.setLong(Replicator.MAX_STORED_SEQNO,
                        pipeline.getMaxStoredSeqno());
                statusProps.setDouble(Replicator.APPLIED_LATENCY,
                        pipeline.getApplyLatency());
            }
        }

        // Fill out non-pipeline data.
        if (extensions == null)
            statusProps.setString(Replicator.EXTENSIONS, "");
        else
            statusProps.setStringList(Replicator.EXTENSIONS, extensions);
        statusProps.setString(Replicator.SERVICE_TYPE, type);

        return statusProps.hashMap();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.management.OpenReplicatorPlugin#statusList(java.lang.String)
     */
    public List<Map<String, String>> statusList(String name) throws Exception
    {
        List<Map<String, String>> statusList = new ArrayList<Map<String, String>>();

        // Fetch the pipeline.
        Pipeline pipeline = null;
        if (runtime != null)
            pipeline = runtime.getPipeline();

        // If we have a pipeline, process the status request.
        if (pipeline != null)
        {
            if ("tasks".equals(name))
            {
                // Fetch task information and put into the list.
                List<TaskProgress> progressList = pipeline.getTaskProgress();
                for (TaskProgress progress : progressList)
                {
                    Map<String, String> props = new HashMap<String, String>();
                    props.put("stage", progress.getStageName());
                    props.put("taskId", Integer.toString(progress.getTaskId()));
                    props.put("cancelled",
                            Boolean.toString(progress.isCancelled()));
                    props.put("eventCount",
                            Long.toString(progress.getEventCount()));
                    long blockCount = progress.getBlockCount();
                    double avgBlock;
                    if (blockCount > 0)
                        avgBlock = (double) progress.getEventCount()
                                / blockCount;
                    else
                        avgBlock = 0.0;
                    props.put("averageBlockSize",
                            String.format("%-10.3f", avgBlock));
                    props.put("appliedLatency",
                            Double.toString(progress.getApplyLatencySeconds()));
                    props.put("extractTime",
                            Double.toString(progress.getTotalExtractSeconds()));
                    props.put("filterTime",
                            Double.toString(progress.getTotalFilterSeconds()));
                    props.put("applyTime",
                            Double.toString(progress.getTotalApplySeconds()));
                    props.put("otherTime",
                            Double.toString(progress.getTotalOtherSeconds()));
                    props.put("state", progress.getState().toString());
                    ReplDBMSHeader lastCommittedEvent = progress
                            .getLastCommittedEvent();
                    if (lastCommittedEvent == null)
                    {
                        props.put("appliedLastSeqno", "-1");
                        props.put("appliedLastEventId", "");
                    }
                    else
                    {
                        props.put("appliedLastSeqno",
                                Long.toString(lastCommittedEvent.getSeqno()));
                        props.put("appliedLastEventId",
                                lastCommittedEvent.getEventId());
                    }
                    ReplDBMSHeader lastDirtyEvent = progress
                            .getLastProcessedEvent();
                    if (lastDirtyEvent == null)
                    {
                        props.put("currentLastSeqno", "-1");
                        props.put("currentLastEventId", "");
                        props.put("currentLastFragno", "-1");
                    }
                    else
                    {
                        props.put("currentLastSeqno",
                                Long.toString(lastDirtyEvent.getSeqno()));
                        props.put("currentLastEventId",
                                lastDirtyEvent.getEventId());
                        props.put("currentLastFragno",
                                Short.toString(lastDirtyEvent.getFragno()));
                    }
                    statusList.add(props);
                }
            }
            else if ("shards".equals(name))
            {
                // Fetch shard information and put into the list.
                List<ShardProgress> progressList = pipeline.getShardProgress();
                for (ShardProgress progress : progressList)
                {
                    Map<String, String> props = new HashMap<String, String>();
                    props.put("shardId", progress.getShardId());
                    props.put("stage", progress.getStageName());
                    props.put("eventCount",
                            Long.toString(progress.getEventCount()));
                    props.put("appliedLatency",
                            Double.toString(progress.getApplyLatencySeconds()));
                    props.put("appliedLastSeqno",
                            Long.toString(progress.getLastSeqno()));
                    props.put("appliedLastEventId", progress.getLastEventId());

                    statusList.add(props);
                }
            }
            else if ("stores".equals(name))
            {
                // Fetch task information and put into the list.
                for (String storeName : pipeline.getStoreNames())
                {
                    Store store = pipeline.getStore(storeName);
                    TungstenProperties storeProps = store.status();
                    storeProps.setString("name", storeName);
                    storeProps.setString("storeClass", store.getClass()
                            .getName());
                    statusList.add(storeProps.hashMap());
                }
            }
            else if ("services".equals(name))
            {
                for (String serviceName : pipeline.getServiceNames())
                {
                    PipelineService service = pipeline.getService(serviceName);
                    TungstenProperties serviceProps = service.status();
                    serviceProps.setString("name", serviceName);
                    serviceProps.setString("storeClass", service.getClass()
                            .getName());
                    statusList.add(serviceProps.hashMap());
                }
            }
            else if ("channel-assignments".equals(name))
            {
                // For this call we need to look for a service that implements
                // shard channel assignments. We then list the current
                // assignments.
                for (String serviceName : pipeline.getServiceNames())
                {
                    PipelineService service = pipeline.getService(serviceName);
                    if (service instanceof ChannelAssignmentService)
                    {
                        ChannelAssignmentService assignmentService = (ChannelAssignmentService) service;
                        List<Map<String, String>> assignments = assignmentService
                                .listChannelAssignments();
                        for (Map<String, String> assignment : assignments)
                        {
                            statusList.add(assignment);
                        }
                    }
                }
            }
            else if ("stages".equals(name))
            {
                // Fetch stage information and put into the list.
                List<Stage> stages = pipeline.getStages();
                for (Stage stage : stages)
                {
                    Map<String, String> props = new HashMap<String, String>();
                    props.put("name", stage.getName());
                    props.put("taskCount",
                            new Integer(stage.getTaskCount()).toString());
                    props.put("blockCommitRowCount",
                            new Integer(stage.getBlockCommitRowCount())
                                    .toString());

                    // Add stage components.
                    props.put("applier.name", stage.getApplierSpec().getName());
                    props.put("applier.class", stage.getApplierSpec()
                            .getPluginClass().getName());
                    props.put("extractor.name", stage.getExtractorSpec()
                            .getName());
                    props.put("extractor.class", stage.getExtractorSpec()
                            .getPluginClass().getName());
                    List<PluginSpecification> filters = stage.getFilterSpecs();
                    for (int i = 0; i < filters.size(); i++)
                    {
                        String prefix = "filter." + i;
                        props.put(prefix + ".name", filters.get(i).getName());
                        props.put(prefix + ".class", filters.get(i)
                                .getPluginClass().getName());
                    }

                    // Print stage progress.
                    StageProgressTracker tracker = stage.getProgressTracker();
                    props.put("committedMinSeqno",
                            new Long(tracker.getCommittedMinSeqno()).toString());
                    props.put("processedMinSeqno",
                            new Long(tracker.getDirtyMinLastSeqno()).toString());

                    statusList.add(props);
                }
            }
            else if ("watches".equals(name))
            {
                List<Stage> stages = pipeline.getStages();
                for (Stage stage : stages)
                {
                    // Fetch committed and processing watches.
                    addWatchStatus(stage, statusList, true);
                    addWatchStatus(stage, statusList, false);
                }
            }

            else
                throw new ReplicatorException("Unrecognized status list type: "
                        + name);
        }

        // Return whatever we found.
        return statusList;
    }

    // Fetch watches and add list status list.
    private void addWatchStatus(Stage stage,
            List<Map<String, String>> statusList, boolean committed)
    {
        List<Watch<?>> watches = stage.getProgressTracker().getWatches(
                committed);
        for (Watch<?> watch : watches)
        {
            // Unfortunately running a "toString()" on the watch is a bit
            // unsightly as it dumps data in a single line. We have to
            // format it ourselves.
            Map<String, String> props = new HashMap<String, String>();
            WatchAction<?> action = watch.getAction();
            props.put("stage", stage.getName());
            props.put("committed", new Boolean(committed).toString());
            props.put("action", action == null ? "none" : action.toString());
            props.put("predicate", watch.getPredicate().toString());
            props.put("cancelled", new Boolean(watch.isCancelled()).toString());
            props.put("done", new Boolean(watch.isDone()).toString());

            boolean[] matched = watch.getMatched();
            StringBuffer matchString = new StringBuffer("[");
            for (int i = 0; i < matched.length; i++)
            {
                if (i > 0)
                    matchString.append(",");
                matchString.append("[").append(i).append(":")
                        .append(matched[i]).append("]");
            }
            matchString.append("]");
            props.put("matched", matchString.toString());
            statusList.add(props);
        }

        /*
         * // Turn the list of matches into a string. StringBuffer matchString =
         * new StringBuffer("["); for (int i = 0; i < matched.length; i++) { if
         * (i > 0) matchString.append(",");
         * matchString.append("[").append(i).append(":").append(matched[i])
         * .append("]"); } matchString.append("]"); return
         * this.getClass().getSimpleName() + " predicate=" +
         * predicate.toString() + " done=" + done + " cancelled=" + cancelled +
         * " matched=" + matchString.toString();
         */
    }

    public void provision(String uri) throws Exception
    {
        // TODO Auto-generated method stub

    }

    public ReplicatorCapabilities getCapabilities() throws Exception
    {
        ReplicatorCapabilities capabilities = new ReplicatorCapabilities();

        capabilities.setModel(ReplicatorCapabilities.MODEL_PUSH);

        capabilities.addRole(ReplicatorCapabilities.ROLE_MASTER);
        capabilities.addRole(ReplicatorCapabilities.ROLE_SLAVE);

        capabilities.setConsistencyCheck(true);
        capabilities.setHeartbeat(true);
        capabilities.setFlush(true);

        capabilities.setProvisionDriver(ReplicatorCapabilities.PROVISION_DONOR);
        return capabilities;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.management.OpenReplicatorPlugin#setRole(java.lang.String,
     *      java.lang.String)
     */
    public void setRole(String role, String uri) throws ReplicatorException
    {
        // No action required. We can ready properties when we restart.
    }

    // Shut down current pipeline, if any.
    private void doShutdown(TungstenProperties params)
            throws ReplicatorException, InterruptedException
    {
        if (pipeline != null)
        {
            pipeline.shutdown(true);
            pipeline = null;
        }
        if (runtime != null)
        {
            runtime.release();
            runtime = null;
        }
    }

    // Release existing runtime, if any, and generate a new one with optional
    // runtime options.
    private void doCreateRuntime(TungstenProperties onlineOptions)
            throws ReplicatorException
    {
        if (runtime != null)
        {
            runtime.release();
        }
        runtime = new ReplicatorRuntime(properties, context,
                ReplicatorMonitor.getInstance());
        if (onlineOptions != null)
        {
            runtime.setOnlineOptions(onlineOptions);
        }
        runtime.configure();
    }

    public ReplicatorRuntime getReplicatorRuntime()
    {
        return runtime;
    }
}