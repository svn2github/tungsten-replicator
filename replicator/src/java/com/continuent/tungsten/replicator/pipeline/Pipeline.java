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
 * Initial developer(s):  Robert Hodges
 * Contributor(s): Stephane Giron
 */

package com.continuent.tungsten.replicator.pipeline;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import com.continuent.tungsten.commons.patterns.event.EventDispatcher;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.applier.Applier;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntime;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.extractor.Extractor;
import com.continuent.tungsten.replicator.management.events.GoOfflineEvent;
import com.continuent.tungsten.replicator.plugin.PluginContext;
import com.continuent.tungsten.replicator.plugin.ReplicatorPlugin;
import com.continuent.tungsten.replicator.service.PipelineService;
import com.continuent.tungsten.replicator.storage.Store;

/**
 * Stores the information related to a replication pipeline, which is a set of
 * independent processing stages.
 * <p>
 * The pipeline life cycle requires that pipelines do not release underlying
 * stages or progress trackers within stages. This is necessary to avoid race
 * conditions for monitoring and status calls, which may call pipelines at
 * various stages of preparation and also following release. To release pipeline
 * resources, clients must drop references to the pipeline itself.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class Pipeline implements ReplicatorPlugin
{
    private static Logger                    logger               = Logger.getLogger(Pipeline.class);
    private PluginContext                    context;
    private String                           name;
    private LinkedList<Stage>                stages               = new LinkedList<Stage>();
    private HashMap<String, Store>           stores               = new HashMap<String, Store>();
    private List<String>                     storeNames           = new ArrayList<String>();
    private HashMap<String, PipelineService> services             = new HashMap<String, PipelineService>();
    private List<String>                     serviceNames         = new ArrayList<String>();
    private boolean                          autoSync             = false;
    private boolean                          syncTHLWithExtractor = true;
    private ExecutorService                  shutdownTaskExec     = Executors
                                                                          .newCachedThreadPool();
    private TreeMap<String, Future<?>>       offlineRequests      = new TreeMap<String, Future<?>>();

    public Pipeline()
    {
    }

    /**
     * Sets the name of this pipeline, which must be unique across all defined
     * pipelines.
     */
    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public boolean isAutoSync()
    {
        return autoSync;
    }

    public void setAutoSync(boolean autoSync)
    {
        this.autoSync = autoSync;
    }

    public void addStage(Stage stage)
    {
        stages.add(stage);
    }

    public void addStore(String name, Store store)
    {
        stores.put(name, store);
        storeNames.add(name);
    }

    public void addService(String name, PipelineService service)
    {
        services.put(name, service);
        serviceNames.add(name);
    }

    public Stage getStage(String name)
    {
        for (Stage stage : stages)
        {
            if (stage.getName().equals(name))
                return stage;
        }
        return null;
    }

    public List<Stage> getStages()
    {
        return stages;
    }

    public PluginContext getContext()
    {
        return context;
    }

    /** Returns extractor at head of pipeline. */
    public Extractor getHeadExtractor()
    {
        Stage s0 = stages.getFirst();
        if (s0 == null)
            return null;
        else
            return s0.getExtractor0();
    }

    /** Returns applier at tail of pipeline. */
    public Applier getTailApplier()
    {
        Stage s0 = stages.getFirst();
        if (s0 == null)
            return null;
        else
            return s0.getApplier0();
    }

    public Store getStore(String name)
    {
        return stores.get(name);
    }

    public List<String> getStoreNames()
    {
        return storeNames;
    }

    public PipelineService getService(String name)
    {
        return services.get(name);
    }

    public List<String> getServiceNames()
    {
        return serviceNames;
    }

    /**
     * Configures pipeline data structures including stages and stores. All
     * pipeline information is accessible after this call.
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        logger.info("Configuring pipeline: " + name);
        this.context = context;
        if (stages.size() == 0)
            throw new ReplicatorException(
                    "Attempt to configure pipeline without any stages");

        // Set auto sync value on the first stage.
        Stage first = stages.getFirst();
        first.setAutoSync(autoSync);

        for (String name : getServiceNames())
        {
            ReplicatorRuntime.configurePlugin(services.get(name), context);
        }
        for (String name : getStoreNames())
        {
            ReplicatorRuntime.configurePlugin(stores.get(name), context);
        }
        for (Stage stage : stages)
        {
            stage.configure(context);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        logger.info("Preparing pipeline: " + name);

        // Services are prepared first.
        for (String name : getServiceNames())
        {
            ReplicatorRuntime.preparePlugin(services.get(name), context);
        }

        // Next we load stores so that they can call on services.
        for (String name : getStoreNames())
        {
            ReplicatorRuntime.preparePlugin(stores.get(name), context);
        }

        // Finally stages are processed last and in reverse order so that they
        // can propagate restart points backwards through the pipeline.
        for (int i = stages.size() - 1; i >= 0; i--)
        {
            Stage stage = stages.get(i);
            stage.prepare(context);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void release(PluginContext context)
    {
        // Release but do not null out information. This allows pipeline
        // monitoring data to remain accessible after release, thereby
        // avoiding race conditions.
        logger.info("Releasing pipeline: " + name);
        for (Stage stage : stages)
        {
            try
            {
                stage.release(context);
            }
            catch (ReplicatorException e)
            {
                logger.warn("Unable to release stage: " + e.getMessage(), e);
            }
        }

        for (String name : getStoreNames())
        {
            ReplicatorRuntime.releasePlugin(stores.get(name), context);
        }
        for (String name : getServiceNames())
        {
            ReplicatorRuntime.releasePlugin(services.get(name), context);
        }
    }

    /**
     * Start pipeline operation. This is called when replication goes online.
     */
    public synchronized void start(EventDispatcher eventDispatcher)
            throws ReplicatorException
    {
        logger.info("Starting pipeline: " + name);
        for (Stage stage : stages)
            stage.start(eventDispatcher);
    }

    /**
     * Stop pipeline operation. This is called when replication goes offline.
     */
    public synchronized void shutdown(boolean immediate)
    {
        // Shut down pipeline.
        logger.info("Shutting down pipeline: " + name);
        for (Stage stage : stages)
            stage.shutdown(immediate);

        // Stop any pending shutdown requests.
        shutdownTaskExec.shutdownNow();
    }

    /**
     * Shuts down after a particular sequence number is applied.
     * 
     * @param seqno Sequence number to watch for
     * @return Returns future to wait for pipeline shutdown
     */
    public Future<Pipeline> shutdownAfterSequenceNumber(long seqno)
            throws InterruptedException, ReplicatorException
    {
        // Queue watches on all stages.
        ArrayList<Future<ReplDBMSHeader>> taskShutdownFutures = new ArrayList<Future<ReplDBMSHeader>>();
        for (int i = 0; i < stages.size(); i++)
        {
            taskShutdownFutures.add(stages.get(i)
                    .watchForProcessedSequenceNumber(seqno, true));
        }

        return scheduleWait("Offline at sequence number: " + seqno,
                taskShutdownFutures);
    }

    /**
     * Shuts down after a particular event ID is applied.
     * 
     * @param eventId Event ID to watch for
     * @return Returns future to wait for pipeline shutdown
     */
    public Future<Pipeline> shutdownAfterEventId(String eventId)
            throws InterruptedException, ReplicatorException
    {
        // Queue watches on all stages.
        ArrayList<Future<ReplDBMSHeader>> taskShutdownFutures = new ArrayList<Future<ReplDBMSHeader>>();
        for (int i = 0; i < stages.size(); i++)
        {
            taskShutdownFutures.add(stages.get(i).watchForProcessedEventId(
                    eventId, true));
        }

        return scheduleWait("Offline at native event ID: " + eventId,
                taskShutdownFutures);
    }

    /**
     * Shuts down after a heartbeat event is seen.
     * 
     * @return Returns future to wait for pipeline shutdown
     */
    public Future<Pipeline> shutdownAfterHeartbeat(String name)
            throws InterruptedException, ReplicatorException
    {
        // Queue watches on all stages.
        ArrayList<Future<ReplDBMSHeader>> taskShutdownFutures = new ArrayList<Future<ReplDBMSHeader>>();
        for (int i = 0; i < stages.size(); i++)
        {
            taskShutdownFutures.add(stages.get(i).watchForProcessedHeartbeat(
                    name, true));
        }

        return scheduleWait("Offline at heartbeat event: " + name,
                taskShutdownFutures);
    }

    /**
     * Shuts down after the replication event timestamp meets or exceeds the
     * argument.
     * 
     * @param timestamp Timestamp value to wait for
     * @return Returns future to wait for pipeline shutdown
     */
    public Future<Pipeline> shutdownAfterTimestamp(Timestamp timestamp)
            throws InterruptedException, ReplicatorException
    {
        // Queue watches on all stages.
        ArrayList<Future<ReplDBMSHeader>> taskShutdownFutures = new ArrayList<Future<ReplDBMSHeader>>();
        for (int i = 0; i < stages.size(); i++)
        {
            taskShutdownFutures.add(stages.get(i).watchForProcessedTimestamp(
                    timestamp, true));
        }
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
        return scheduleWait("Offline at time: " + sdf.format(timestamp),
                taskShutdownFutures);
    }

    // Enqueue a future to wait for task completion.
    private Future<Pipeline> scheduleWait(String name,
            List<Future<ReplDBMSHeader>> taskFutures)
    {
        DeferredShutdownTask shutdownCallable = new DeferredShutdownTask(this,
                taskFutures);
        Future<Pipeline> future = shutdownTaskExec.submit(shutdownCallable);
        synchronized (offlineRequests)
        {
            offlineRequests.put(name, future);
        }
        return future;
    }

    /**
     * Returns true if the pipeline has stopped.
     */
    public boolean isShutdown()
    {
        for (Stage stage : stages)
        {
            if (!stage.isShutdown())
                return false;
        }
        return true;
    }

    /** Returns the last value processed in the first stage. */
    public long getLastExtractedSeqno()
    {
        return stages.getFirst().getProgressTracker().getDirtyMinLastSeqno();
    }

    /** Returns the last sequence number applied in the last stage. */
    public long getLastAppliedSeqno()
    {
        return stages.getLast().getProgressTracker().getCommittedMinSeqno();
    }

    /** Returns the last event applied in the last stage. */
    public ReplDBMSHeader getLastAppliedEvent()
    {
        return stages.getLast().getProgressTracker().getCommittedMinEvent();
    }

    /** Returns the latency of applying the last committed event in seconds. */
    public double getApplyLatency()
    {
        long applyLatencyMillis = stages.getLast().getProgressTracker()
                .getCommittedApplyLatency();
        return applyLatencyMillis / 1000.0;
    }

    /**
     * Returns the current minimum stored sequence number.
     */
    public long getMinStoredSeqno()
    {
        long seqno = -1;
        for (Store store : stores.values())
        {
            // First term in predicate ensures that we assign a value.
            long minStoredSeqno = store.getMinStoredSeqno();
            if (seqno == -1 || seqno > minStoredSeqno)
                seqno = minStoredSeqno;
        }
        return seqno;
    }

    /**
     * Returns the current maximum stored sequence number.
     */
    public long getMaxStoredSeqno()
    {
        long seqno = -1;
        for (Store store : stores.values())
        {
            long maxStoredSeqno = store.getMaxStoredSeqno();
            if (seqno < maxStoredSeqno)
                seqno = maxStoredSeqno;
        }
        return seqno;
    }

    /**
     * Returns a formatted list of current offline requests.
     */
    public String getOfflineRequests()
    {
        synchronized (offlineRequests)
        {
            StringBuffer sb = new StringBuffer();
            for (String request : offlineRequests.keySet())
            {
                if (sb.length() > 0)
                    sb.append(';');
                sb.append(request);
            }
            return sb.toString();
        }
    }

    /**
     * Returns task progress instances ordered by task ID.
     */
    public synchronized List<TaskProgress> getTaskProgress()
    {
        List<TaskProgress> progressList = new ArrayList<TaskProgress>();
        for (Stage stage : stages)
        {
            progressList.addAll(stage.getTaskProgress());
        }
        return progressList;
    }

    /**
     * Returns shard progress instances ordered by shard ID. Shard progress is
     * measured from the end of the pipeline, so we fetch it from the final task
     * only.
     */
    public synchronized List<ShardProgress> getShardProgress()
    {
        return stages.getLast().getShardProgress();
    }

    /**
     * Sets the native event ID from which to start extracting. This overrides
     * the default value obtained from the applier at the end of the pipeline.
     * Must be called before start() to have an effect.
     * 
     * @param eventId Event ID from which to start replication
     */
    public void setInitialEventId(String eventId)
    {
        stages.getFirst().setInitialEventId(eventId);
    }

    /**
     * Sets the number of apply transactions to skip, which allows the pipeline
     * to skip over errors.
     */
    public void setApplySkipCount(long skipCount)
    {
        stages.getLast().setApplySkipCount(skipCount);
    }

    /**
     * Sets a watch for a particular sequence number to be extracted.
     * 
     * @param seqno Sequence number to watch for
     * @return Returns a watch on a corresponding event
     * @throws InterruptedException if cancelled
     */
    public Future<ReplDBMSHeader> watchForExtractedSequenceNumber(long seqno)
            throws InterruptedException
    {
        return stages.getFirst().watchForProcessedSequenceNumber(seqno, false);
    }

    /**
     * Sets a watch for a particular event ID to be extracted.
     * 
     * @param eventId Native event ID to watch for
     * @return Returns a watch on a corresponding event
     * @throws InterruptedException if cancelled
     */
    public Future<ReplDBMSHeader> watchForExtractedEventId(String eventId)
            throws InterruptedException
    {
        return stages.getFirst().watchForProcessedEventId(eventId, false);
    }

    /**
     * Sets a watch for a particular sequence number to be applied.
     * 
     * @param seqno Sequence number to watch for
     * @return Returns a future on the event that meets criterion
     * @throws InterruptedException if cancelled
     */
    public Future<ReplDBMSHeader> watchForAppliedSequenceNumber(long seqno)
            throws InterruptedException
    {
        return stages.getLast().watchForProcessedSequenceNumber(seqno, false);
    }

    /**
     * Sets a watch for a particular event ID to be applied.
     * 
     * @param eventId Native event ID to watch for
     * @return Returns a watch on a corresponding event
     * @throws InterruptedException if canceled
     */
    public Future<ReplDBMSHeader> watchForAppliedEventId(String eventId)
            throws InterruptedException
    {
        return stages.getLast().watchForProcessedEventId(eventId, false);
    }

    /**
     * Find the current native event ID in the DBMS and wait until it reaches
     * the log.
     * 
     * @return A Future on the ReplDBMSEvent that has this eventId or a greater
     *         one.
     * @throws InterruptedException
     * @throws ReplicatorException
     */
    public Future<ReplDBMSHeader> flush() throws InterruptedException,
            ReplicatorException
    {
        Extractor extractor = stages.getFirst().getExtractor0();
        String currentEventId = extractor.getCurrentResourceEventId();
        return watchForAppliedEventId(currentEventId);
    }

    public void setSyncTHLWithExtractor(boolean syncTHLWithExtractor)
    {
        this.syncTHLWithExtractor = syncTHLWithExtractor;
    }

    public boolean syncTHLWithExtractor()
    {
        return syncTHLWithExtractor;
    }

    public void setApplySkipEvents(SortedSet<Long> seqnos)
    {
        stages.getLast().setApplySkipEvents(seqnos);
    }

    /**
     * getMaxCommittedSeqno returns the max committed sequence number from all
     * the stores.
     * 
     * @return the max committed seqno
     * @throws ReplicatorException in case an error occurs
     */
    public long getMaxCommittedSeqno() throws ReplicatorException
    {
        long seqno = -1;
        for (Store store : stores.values())
        {
            long maxCommittedSeqno = store.getMaxStoredSeqno();
            if (seqno < maxCommittedSeqno)
                seqno = maxCommittedSeqno;
        }
        return seqno;
    }
}

// Interruptible task to wait for stage tasks to read a watch point.
class DeferredShutdownTask implements Callable<Pipeline>
{
    private static final Logger                logger = Logger.getLogger(DeferredShutdownTask.class);
    private final Pipeline                     pipeline;
    private final List<Future<ReplDBMSHeader>> taskWaits;

    DeferredShutdownTask(Pipeline pipeline,
            List<Future<ReplDBMSHeader>> taskWaits)
    {
        this.pipeline = pipeline;
        this.taskWaits = taskWaits;
    }

    /**
     * Returns when the pipeline is safely shut down. {@inheritDoc}
     * 
     * @see java.util.concurrent.Callable#call()
     */
    public Pipeline call() throws Exception
    {
        logger.info("Waiting for pipeline to shut down: " + pipeline.getName());

        try
        {
            // Wait for tasks to hit the event on which we are waiting.
            for (Future<ReplDBMSHeader> taskWait : taskWaits)
            {
                ReplDBMSHeader event = taskWait.get();
                if (logger.isDebugEnabled())
                {
                    logger.debug("Reached event: " + event.getSeqno());
                }
            }

            // Ensure that all tasks have terminated. Tasks can take
            // a little extra time to finish after hitting the last event.
            int waitCount = 0;
            while (!pipeline.isShutdown())
            {
                waitCount++;
                if (waitCount % 1000 == 0)
                {
                    // It's nice to tell people what's going on if we hang for
                    // some reason.
                    logger.info("Waiting for pipeline to shut down fully...");
                }
                Thread.sleep(10);
            }
        }
        catch (InterruptedException e)
        {
            logger.warn("Pipeline shutdown wait was interrupted");
        }

        logger.info("Pipeline has shut down, dispatching offline event: "
                + pipeline.getName());
        // TODO: This probably should not be here--pipelines should not know
        // about the state machine.
        pipeline.getContext().getEventDispatcher().put(new GoOfflineEvent());
        return pipeline;
    }
}