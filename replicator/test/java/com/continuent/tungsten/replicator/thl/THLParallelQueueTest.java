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
 * Contributor(s): 
 */

package com.continuent.tungsten.replicator.thl;

import java.io.File;
import java.io.FileInputStream;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.continuent.tungsten.commons.config.TungstenProperties;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.applier.DummyApplier;
import com.continuent.tungsten.replicator.conf.ReplicatorConf;
import com.continuent.tungsten.replicator.conf.ReplicatorMonitor;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntime;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.event.ReplOptionParams;
import com.continuent.tungsten.replicator.extractor.DummyExtractor;
import com.continuent.tungsten.replicator.management.MockEventDispatcher;
import com.continuent.tungsten.replicator.management.MockOpenReplicatorContext;
import com.continuent.tungsten.replicator.pipeline.Pipeline;
import com.continuent.tungsten.replicator.pipeline.PipelineConfigBuilder;
import com.continuent.tungsten.replicator.storage.CommitAction;
import com.continuent.tungsten.replicator.storage.InMemoryMultiQueue;
import com.continuent.tungsten.replicator.storage.InMemoryMultiQueueApplier;
import com.continuent.tungsten.replicator.storage.InMemoryTransactionalQueue;
import com.continuent.tungsten.replicator.storage.InMemoryTransactionalQueueApplier;
import com.continuent.tungsten.replicator.storage.Store;
import com.continuent.tungsten.replicator.storage.parallel.HashPartitioner;
import com.continuent.tungsten.replicator.thl.log.LogConnection;

/**
 * Implements a test of parallel THL operations. Parallel THL operation requires
 * a pipeline consisting of a THL coupled with a THLParallelQueue.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class THLParallelQueueTest
{
    private static Logger             logger = Logger.getLogger(THLParallelQueueTest.class);
    private static TungstenProperties testProperties;

    /** Each test uses this pipeline and runtime. */
    private Pipeline                  pipeline;
    private ReplicatorRuntime         runtime;

    /**
     * Make sure we have expected test properties.
     * 
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        // Set test.properties file name.
        String testPropertiesName = System.getProperty("test.properties");
        if (testPropertiesName == null)
        {
            testPropertiesName = "test.properties";
            logger.info("Setting test.properties file name to default: test.properties");
        }

        // Load properties file.
        testProperties = new TungstenProperties();
        File f = new File(testPropertiesName);
        if (f.canRead())
        {
            logger.info("Reading test.properties file: " + testPropertiesName);
            FileInputStream fis = new FileInputStream(f);
            testProperties.load(fis);
            fis.close();
        }
    }

    /**
     * Shut down pipeline at end of test.
     */
    @After
    public void teardown()
    {
        if (pipeline != null)
        {
            logger.info("Shutting down pipeline...");
            pipeline.shutdown(false);
        }
        if (runtime != null)
        {
            logger.info("Releasing runtime...");
            runtime.release();
        }
    }

    /*
     * Verify that we can start and stop a pipeline containing a THL with a
     * THLParallelQueue.
     */
    @Test
    public void testPipelineStartStop() throws Exception
    {
        logger.info("##### testPipelineStartStop #####");

        // Set up and start pipelines.
        TungstenProperties conf = this.generateTHLParallelQueueProps(
                "testPipelineStartStop", 1);
        runtime = new ReplicatorRuntime(conf, new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();
        pipeline = runtime.getPipeline();
        pipeline.start(new MockEventDispatcher());

        // Close down pipeline.
        pipeline.shutdown(false);
        runtime.release();
    }

    /*
     * Verify that a pipeline with a single channel successfully transmits
     * events from end to end.
     */
    @Test
    public void testSingleChannel() throws Exception
    {
        logger.info("##### testSingleChannel #####");

        // Set up and start pipelines.
        TungstenProperties conf = this.generateTHLParallelQueueProps(
                "testSingleChannel", 1);
        runtime = new ReplicatorRuntime(conf, new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();
        pipeline = runtime.getPipeline();
        pipeline.start(new MockEventDispatcher());

        // Wait for and verify events.
        Future<ReplDBMSHeader> wait = pipeline.watchForAppliedSequenceNumber(9);
        ReplDBMSHeader lastEvent = wait.get(5, TimeUnit.SECONDS);
        Assert.assertEquals("Expected 10 server events", 9,
                lastEvent.getSeqno());

        Store thl = pipeline.getStore("thl");
        Assert.assertEquals("Expected 0 as first event", 0,
                thl.getMinStoredSeqno());
        Assert.assertEquals("Expected 9 as last event", 9,
                thl.getMaxStoredSeqno());
    }

    /*
     * Verify that a pipeline with multiple channels successfully transmits
     * events from end to end.
     */
    @Test
    public void testMultipleChannels() throws Exception
    {
        logger.info("##### testMultipleChannels #####");

        // Set up and start pipelines.
        TungstenProperties conf = this.generateTHLParallelQueueProps(
                "testMultipleChannels", 1);
        runtime = new ReplicatorRuntime(conf, new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();
        pipeline = runtime.getPipeline();
        pipeline.start(new MockEventDispatcher());

        // Wait for and verify events.
        Future<ReplDBMSHeader> wait = pipeline.watchForAppliedSequenceNumber(9);
        ReplDBMSHeader lastEvent = wait.get(5, TimeUnit.SECONDS);
        Assert.assertEquals("Expected 10 server events", 9,
                lastEvent.getSeqno());

        Store thl = pipeline.getStore("thl");
        Assert.assertEquals("Expected 0 as first event", 0,
                thl.getMinStoredSeqno());
        Assert.assertEquals("Expected 9 as last event", 9,
                thl.getMaxStoredSeqno());
    }

    /**
     * Verify that multiple channels correctly "stratify" serialized and
     * non-serialized transactions into ordered groups. We do so by adding some
     * randomization to the commit times on a transactional in-memory queue,
     * which helps make serialization errors easier to see. This simulates a
     * DBMS with slow commits.
     */
    @Test
    public void testMultiChannelSerialization() throws Exception
    {
        logger.info("##### testMultiChannelSerialization #####");
        int maxEvents = 25;

        // Make sure that this case is enabled.
        boolean enabled = testProperties
                .getBoolean("testMultiChannelSerialization");
        if (!enabled)
        {
            logger.info("Test case is not enabled...Skipping it!");
            return;
        }

        // Set up and prepare pipeline. We set the channel count to
        // 1 as we just want to confirm that serialization counts are
        // increasing.
        TungstenProperties conf = this.generateTHLParallelPipeline(
                "testMultiChannelSerialization", 3, 50, 1000, false);
        runtime = new ReplicatorRuntime(conf, new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();
        pipeline = runtime.getPipeline();
        pipeline.start(new MockEventDispatcher());

        // Fetch references to stores.
        THL thl = (THL) pipeline.getStore("thl");
        THLParallelQueue tpq = (THLParallelQueue) pipeline
                .getStore("thl-queue");
        InMemoryTransactionalQueue mq = (InMemoryTransactionalQueue) pipeline
                .getStore("multi-queue");

        // Add a commit action that randomizes the time to commit. This
        // will stimulate race conditions if there is bad coordination
        // between serialized and non-serialized events.
        CommitAction ca = new CommitAction()
        {
            public void execute(int taskId) throws ReplicatorException
            {
                // Randomly wait up to 99ms
                long waitMillis = (long) (100 * Math.random());
                try
                {
                    Thread.sleep(waitMillis);
                }
                catch (InterruptedException e)
                {
                    logger.info("Unexpected interruption on commit", e);
                }
            }
        };
        mq.setCommitAction(ca);

        // Write events where every third event is #UNKNOWN, thereby
        // forcing serialization.
        int serialized = 0;
        LogConnection conn = thl.connect(false);
        for (int i = 0; i < maxEvents; i++)
        {
            // Generate the event.
            String shardId;
            int id = (i + 1) % 3;
            if (id == 0)
            {
                shardId = "#UNKNOWN";
                serialized++;
            }
            else
                shardId = "db" + id;

            // Write same to the log.
            ReplDBMSEvent rde = this.createEvent(i, shardId);
            THLEvent thlEvent = new THLEvent(rde.getSourceId(), rde);
            conn.store(thlEvent, false);
            conn.commit();
        }
        thl.disconnect(conn);

        // Wait for the last event to commit and then ensure we
        // serialized the expected number of times.
        Future<ReplDBMSHeader> committed = pipeline
                .watchForCommittedSequenceNumber(maxEvents - 1, false);
        committed.get(30, TimeUnit.SECONDS);

        int actualSerialized = getSerializationCount(tpq);
        Assert.assertEquals("Checking expected serialization count",
                serialized, actualSerialized);

        // Read through the events in the serialized queue and ensure they
        // are properly stratified. Basically the serialized #UNKNOWN
        // shards must be in total order, between them the db0 and db1 shards
        // can come in either order.
        Map<String, Long> dbHash = new HashMap<String, Long>();
        long lastSerializedSeqno = -1;
        for (int i = 0; i < maxEvents; i++)
        {
            ReplDBMSEvent rde2 = mq.get();
            long seqno = rde2.getSeqno();
            String shardId = rde2.getShardId();
            logger.info("Read event: seqno=" + seqno + " shardId=" + shardId);
            if (shardId.equals("#UNKNOWN"))
            {
                // If we are on a serialized shard there must be a db1 or db2
                // in the hash map unless we are on the first iteration.
                if (i > 0)
                {
                    Assert.assertEquals(
                            "Checking preceding events for serialized seqno="
                                    + seqno, 2, dbHash.size());
                }

                // Prepare for the next round of unordered updates on shards
                lastSerializedSeqno = seqno;
                dbHash.clear();
            }
            else
            {
                // Must be an unserialized shard. Ensure it is within 2 numbers
                // of the last serialized seqno.
                if (seqno <= lastSerializedSeqno
                        || seqno > lastSerializedSeqno + 2)
                {
                    throw new Exception(
                            "Serialization violation; non-serialized event "
                                    + "not within 2 numbers of serialized event: lastSerializedSeqno="
                                    + lastSerializedSeqno
                                    + " non-serialized event seqno=" + seqno);
                }
                else
                {
                    dbHash.put(rde2.getShardId(), seqno);
                }
            }
        }
    }

    /**
     * Verify that on-disk queues increment the serialization count each time a
     * serialized event is processed.
     */
    @Test
    public void testSerialization() throws Exception
    {
        logger.info("##### testSerialization #####");

        // Set up and prepare pipeline. We set the channel count to
        // 1 as we just want to confirm that serialization counts are
        // increasing.
        TungstenProperties conf = this.generateTHLParallelPipeline(
                "testSerialization", 1, 50, 100, false);
        runtime = new ReplicatorRuntime(conf, new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();
        pipeline = runtime.getPipeline();
        pipeline.start(new MockEventDispatcher());

        // Fetch references to stores.
        THL thl = (THL) pipeline.getStore("thl");
        THLParallelQueue tpq = (THLParallelQueue) pipeline
                .getStore("thl-queue");
        InMemoryTransactionalQueue mq = (InMemoryTransactionalQueue) pipeline
                .getStore("multi-queue");

        // Write and read back 33 events where every third event is #UNKNOWN,
        // hence should be serialized by the HashSerializer class.
        int serialized = 0;
        LogConnection conn = thl.connect(false);
        for (int i = 0; i < 33; i++)
        {
            // Get the serialization count from the store.
            int serializationCount = getSerializationCount(tpq);

            // Insert and read back an event from the end of the pipeline.
            String shardId = (i % 3 == 0 ? "#UNKNOWN" : "db0");
            ReplDBMSEvent rde = this.createEvent(i, shardId);
            THLEvent thlEvent = new THLEvent(rde.getSourceId(), rde);
            conn.store(thlEvent, false);
            conn.commit();
            ReplDBMSEvent rde2 = mq.get();
            logger.info("Read event: seqno=" + rde2.getSeqno() + " shardId="
                    + rde2.getShardId());

            // Ensure that we got the event back and that the serialization
            // count incremented by one *only* for #UNKNOWN events.
            Assert.assertEquals("Read back same event", rde.getSeqno(),
                    rde2.getSeqno());
            int serializationCount2 = getSerializationCount(tpq);
            if ("#UNKNOWN".equals(rde.getShardId()))
            {
                serialized++;
                Assert.assertEquals("Expect serialization to increment",
                        serializationCount + 1, serializationCount2);
            }
            else
            {
                Assert.assertEquals("Expect serialization to remain the same",
                        serializationCount, serializationCount2);
            }
        }
        thl.disconnect(conn);

        // Ensure we serialized 11 (= 33 / 3) events in total.
        Assert.assertEquals("Serialization total", 11, serialized);
    }

    /**
     * Verify that a parallel THL queue with more than one partition assigns
     * each event to the correct channel. This test uses 3 channels with
     * partitioning on shard name. We write and read directly to/from the linked
     * THL and THLParallelQueue to confirm behavior.
     */
    @Test
    public void testMultiChannelBasic() throws Exception
    {
        logger.info("##### testMultiChannelBasic #####");

        // Set up and prepare pipeline.
        TungstenProperties conf = this.generateTHLParallelPipeline(
                "testMultiChannelBasic", 3, 50, 100, true);
        runtime = new ReplicatorRuntime(conf, new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();
        pipeline = runtime.getPipeline();
        pipeline.start(new MockEventDispatcher());

        // Fetch references to stores.
        THL thl = (THL) pipeline.getStore("thl");
        InMemoryMultiQueue mq = (InMemoryMultiQueue) pipeline
                .getStore("multi-queue");

        // Write events to the THL with three different shard IDs.
        LogConnection conn = thl.connect(false);
        for (int i = 0; i < 90; i++)
        {
            ReplDBMSEvent rde = this.createEvent(i, "db" + (i % 3));
            THLEvent thlEvent = new THLEvent(rde.getSourceId(), rde);
            conn.store(thlEvent, false);
        }
        conn.commit();
        thl.disconnect(conn);

        // Confirm that each parallel queue on the other side gets 30 events and
        // that said events are partially ordered within each queue.
        for (int q = 0; q < 3; q++)
        {
            long seqno = -1;
            String shardId = "db" + q;
            for (int i = 0; i < 30; i++)
            {
                ReplDBMSEvent rde2 = (ReplDBMSEvent) mq.get(q);
                Assert.assertTrue("Seqno increases due to partial order",
                        rde2.getSeqno() > seqno);
                Assert.assertEquals("Shard ID matches queue", shardId,
                        rde2.getShardId());
            }
        }
    }

    /**
     * Confirm that watch synchronization control events go to a single
     * partition and appear in total order compared to all other events. We
     * implement this test by inserting watch events on even sequence numbers
     * then picking them out from queues.
     */
    @Test
    public void testSinglePartitionWatch() throws Exception
    {
        logger.info("##### testSinglePartitionWatch #####");

        // Set up and prepare pipeline. We set the channel count to
        // 1 as we just want to confirm that watches are working.
        TungstenProperties conf = this.generateTHLParallelPipeline(
                "testSinglePartitionWatch", 1, 50, 200, true);
        runtime = new ReplicatorRuntime(conf, new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();
        pipeline = runtime.getPipeline();
        pipeline.start(new MockEventDispatcher());

        // Fetch references to stores.
        THL thl = (THL) pipeline.getStore("thl");
        InMemoryMultiQueue mq = (InMemoryMultiQueue) pipeline
                .getStore("multi-queue");

        // Define arrays of futures to hold watch results. We want to check
        // processing and commit watches, hence have two arrays.
        List<Future<ReplDBMSHeader>> processing = new ArrayList<Future<ReplDBMSHeader>>(
                10);
        List<Future<ReplDBMSHeader>> commits = new ArrayList<Future<ReplDBMSHeader>>(
                10);
        for (int i = 0; i < 10; i++)
        {
            // Seqno is every 10 positions.
            long seqno = i * 10;
            processing.add(pipeline.watchForAppliedSequenceNumber(seqno));
            commits.add(pipeline.watchForAppliedSequenceNumber(seqno));
        }

        // Write 100 events, which should trigger the watches. Commit
        // at intervals so things show up in batches.
        LogConnection conn = thl.connect(false);
        for (int i = 0; i < 100; i++)
        {
            // Insert and read back an event from the end of the pipeline.
            ReplDBMSEvent rde = this.createEvent(i, "db0");
            THLEvent thlEvent = new THLEvent(rde.getSourceId(), rde);
            conn.store(thlEvent, false);

            // Commit every third event.
            if (i % 3 == 0)
                conn.commit();
        }
        conn.commit();
        thl.disconnect(conn);

        // Read 100 events. Ensure the events follow the expected seqno
        // sequence.
        for (int i = 0; i < 100; i++)
        {
            ReplDBMSEvent rde2 = mq.get(0);
            Assert.assertEquals("Checking sequence number", i, rde2.getSeqno());
        }

        // Now check that all watches fired. Processing watches should fire
        // on the exact sequence number. Commit watches may commit afterwards.
        logger.info("Checking watches to ensure they fired at expected points");
        for (int i = 0; i < 10; i++)
        {
            // Seqno is every 10 positions.
            long seqno = i * 10;
            ReplDBMSHeader processed = processing.get(i).get(10,
                    TimeUnit.SECONDS);
            logger.info("Processing watch [" + i + "]: watch seqno=" + seqno
                    + " trigger seqno=" + processed.getSeqno());
            Assert.assertEquals("Processing watches must match exact seqno",
                    seqno, processed.getSeqno());
            ReplDBMSHeader committed = commits.get(i).get(10, TimeUnit.SECONDS);
            logger.info("Committed watch [" + i + "]: watch seqno=" + seqno
                    + " trigger seqno=" + committed.getSeqno());
            Assert.assertTrue(
                    "Commit watches may fire on equal or greater seqno",
                    seqno >= committed.getSeqno());
        }
    }

    /**
     * Verify that a parallel THL queue with more than one partition allows
     * reads from one partition even when another partition is filled to
     * capacity. This proves that the parallel THL queue can handle a very large
     * gap between the positions of different partitions.
     */
    @Test
    public void testLaggingChannels() throws Exception
    {
        logger.info("##### testLaggingChannels #####");

        // Set up and prepare pipeline.
        TungstenProperties conf = this.generateTHLParallelPipeline(
                "testLaggingChannels", 3, 50, 100, true);
        runtime = new ReplicatorRuntime(conf, new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();
        pipeline = runtime.getPipeline();
        pipeline.start(new MockEventDispatcher());

        // Fetch references to stores.
        THL thl = (THL) pipeline.getStore("thl");
        InMemoryMultiQueue mq = (InMemoryMultiQueue) pipeline
                .getStore("multi-queue");

        // Write a large number of events on an initial shard ID. This should be
        // far greater than the maxSize parameter of the queue.
        LogConnection conn = thl.connect(false);
        logger.info("Writing db0 events");
        for (int i = 0; i < 100000; i++)
        {
            ReplDBMSEvent rde = this.createEvent(i, "db0");
            THLEvent thlEvent = new THLEvent(rde.getSourceId(), rde);
            conn.store(thlEvent, false);
        }
        conn.commit();

        // Write 100 events on a second shard ID.
        logger.info("Writing db1 events");
        for (int i = 100000; i < 100100; i++)
        {
            ReplDBMSEvent rde = this.createEvent(i, "db1");
            THLEvent thlEvent = new THLEvent(rde.getSourceId(), rde);
            conn.store(thlEvent, false);
        }
        conn.commit();
        thl.disconnect(conn);

        // Read the 100 events on queue 1 first and confirm seqno as well as
        // shard ID.
        logger.info("Reading db1 events");
        for (int i = 100000; i < 100100; i++)
        {
            ReplDBMSEvent rde2 = (ReplDBMSEvent) mq.get(1);
            Assert.assertEquals("Seqno matches expected for this queue", i,
                    rde2.getSeqno());
            Assert.assertEquals("Shard ID matches queue", "db1",
                    rde2.getShardId());
        }

        // Now read the remaining events on queue 0.
        logger.info("Reading db0 events");
        for (int i = 0; i < 100000; i++)
        {
            ReplDBMSEvent rde3 = (ReplDBMSEvent) mq.get(0);
            Assert.assertEquals("Seqno matches expected for this queue", i,
                    rde3.getSeqno());
            Assert.assertEquals("Shard ID matches queue", "db0",
                    rde3.getShardId());
            if (i % 10000 == 0)
                logger.info("Current seqno: " + rde3.getSeqno());
        }
    }

    /**
     * Verify that the parallel queue correctly transfers data in pipeline where
     * only a few of many channels are actually used.
     */
    @Test
    public void testMultiChannelLag() throws Exception
    {
        logger.info("##### testMultiChannelLag #####");

        // Set up and prepare pipeline.
        TungstenProperties conf = this.generateTHLParallelPipeline(
                "testMultiChannelLag", 30, 50, 100, true);
        runtime = new ReplicatorRuntime(conf, new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();
        pipeline = runtime.getPipeline();
        pipeline.start(new MockEventDispatcher());

        // Fetch references to stores.
        THL thl = (THL) pipeline.getStore("thl");
        InMemoryMultiQueue mq = (InMemoryMultiQueue) pipeline
                .getStore("multi-queue");

        // Write a large number of events into the THL using only 3 shards.
        String[] shardNames = {"db01", "db07", "db09"};
        LogConnection conn = thl.connect(false);
        long seqno = 0;
        for (int i = 0; i < 50000; i++)
        {
            for (int shard = 0; shard < 3; shard++)
            {
                ReplDBMSEvent rde = this
                        .createEvent(seqno++, shardNames[shard]);
                THLEvent thlEvent = new THLEvent(rde.getSourceId(), rde);
                conn.store(thlEvent, false);
            }
        }
        conn.commit();
        thl.disconnect(conn);

        // Read across each queue until we reach 100K events for the main
        // shards (i.e., 150K total). Time out after 60 seconds to avoid hangs.
        int shardTotal = 0;
        while (shardTotal < 150000)
        {
            // Iterate across all queues.
            for (int q = 0; q < 30; q++)
            {
                // If the current queue has something in it...
                while (mq.peek(q) != null)
                {
                    // Read next event from this queue.
                    ReplDBMSEvent event = mq.get(q);
                    String shard = event.getShardId();

                    // If it's from a shard we are tracking, count it.
                    for (String shardName : shardNames)
                    {
                        if (shardName.equals(shard))
                            shardTotal++;

                        if (shardTotal % 30000 == 0)
                            logger.info("Tracked shard entries read:"
                                    + shardTotal);
                    }
                }
            }
        }
    }

    // Returns the current serialization count from a parallel queue.
    private int getSerializationCount(THLParallelQueue tpq)
    {
        TungstenProperties props = tpq.status();
        return props.getInt("serializationCount");
    }

    // Generate configuration properties for a three stage-pipeline
    // that loads events into a THL then loads a parallel queue. Input
    // is from a dummy extractor.
    public TungstenProperties generateTHLParallelQueueProps(String schemaName,
            int channels) throws Exception
    {
        // Clear the THL log directory.
        prepareLogDir(schemaName);

        // Create pipeline.
        PipelineConfigBuilder builder = new PipelineConfigBuilder();
        builder.setProperty(ReplicatorConf.SERVICE_NAME, "test");
        builder.setRole("master");
        builder.setProperty(ReplicatorConf.METADATA_SCHEMA, schemaName);
        builder.addPipeline("master", "extract,feed,apply", "thl,thl-queue");
        builder.addStage("extract", "dummy", "thl-apply", null);
        builder.addStage("feed", "thl-extract", "thl-queue-apply", null);
        builder.addStage("apply", "thl-queue-extract", "dummy", null);

        // Define stores.
        builder.addComponent("store", "thl", THL.class);
        builder.addProperty("store", "thl", "logDir", schemaName);
        builder.addComponent("store", "thl-queue", THLParallelQueue.class);
        builder.addProperty("store", "thl-queue", "maxSize", "5");

        // Extract stage components.
        builder.addComponent("extractor", "dummy", DummyExtractor.class);
        builder.addProperty("extractor", "dummy", "nFrags", "1");
        builder.addComponent("applier", "thl-apply", THLStoreApplier.class);
        builder.addProperty("applier", "thl-apply", "storeName", "thl");

        // Feed stage components.
        builder.addComponent("extractor", "thl-extract",
                THLStoreExtractor.class);
        builder.addProperty("extractor", "thl-extract", "storeName", "thl");
        builder.addComponent("applier", "thl-queue-apply",
                THLParallelQueueApplier.class);
        builder.addProperty("applier", "thl-queue-apply", "storeName",
                "thl-queue");

        // Apply stage components.
        builder.addComponent("extractor", "thl-queue-extract",
                THLParallelQueueExtractor.class);
        builder.addProperty("extractor", "thl-queue-extract", "storeName",
                "thl-queue");
        builder.addComponent("applier", "dummy", DummyApplier.class);

        return builder.getConfig();
    }

    // Generate configuration properties for a two-stage pipeline that
    // connects a THL to a THLParallelQueue to an in-memory multi queue, which
    // can mimic parallel apply on DBMS instances. Clients use direct calls
    // to the stores to write to and read from the pipeline.
    //
    // Note that we support two types of parallel queues for testing.
    // Multi-queues keep transactions in separate queues per channel.
    // Transactional queues serialize them in a manner analogous to a DBMS.
    public TungstenProperties generateTHLParallelPipeline(String schemaName,
            int partitions, int blockCommit, int mqSize, boolean multiQueue)
            throws Exception
    {
        // Clear the THL log directory.
        prepareLogDir(schemaName);

        // Convert values to strings so we can use them.
        String partitionsAsString = new Integer(partitions).toString();
        String blockCommitAsString = new Integer(blockCommit).toString();

        // Create pipeline.
        PipelineConfigBuilder builder = new PipelineConfigBuilder();
        builder.setProperty(ReplicatorConf.SERVICE_NAME, "test");
        builder.setRole("master");
        builder.setProperty(ReplicatorConf.METADATA_SCHEMA, schemaName);
        builder.addPipeline("master", "feed1, feed2",
                "thl,thl-queue, multi-queue");

        // Define stores.
        builder.addComponent("store", "thl", THL.class);
        builder.addProperty("store", "thl", "logDir", schemaName);
        builder.addComponent("store", "thl-queue", THLParallelQueue.class);
        builder.addProperty("store", "thl-queue", "maxSize", "100");
        builder.addProperty("store", "thl-queue", "partitions", new Integer(
                partitions).toString());
        builder.addProperty("store", "thl-queue", "partitionerClass",
                HashPartitioner.class.getName());
        if (multiQueue)
            builder.addComponent("store", "multi-queue",
                    InMemoryMultiQueue.class);
        else
            builder.addComponent("store", "multi-queue",
                    InMemoryTransactionalQueue.class);
        builder.addProperty("store", "multi-queue", "maxSize", new Integer(
                mqSize).toString());
        builder.addProperty("store", "multi-queue", "partitions",
                partitionsAsString);

        // Feed1 stage components.
        builder.addStage("feed1", "thl-extract", "thl-queue-apply", null);
        builder.addProperty("stage", "feed1", "blockCommitRowCount",
                blockCommitAsString);
        builder.addComponent("extractor", "thl-extract",
                THLStoreExtractor.class);
        builder.addProperty("extractor", "thl-extract", "storeName", "thl");
        builder.addComponent("applier", "thl-queue-apply",
                THLParallelQueueApplier.class);
        builder.addProperty("applier", "thl-queue-apply", "storeName",
                "thl-queue");

        // Feed2 stage components.
        builder.addStage("feed2", "thl-queue-extract", "multi-queue-apply",
                null);
        builder.addProperty("stage", "feed2", "taskCount", partitionsAsString);
        builder.addProperty("stage", "feed2", "blockCommitRowCount",
                blockCommitAsString);
        builder.addComponent("extractor", "thl-queue-extract",
                THLParallelQueueExtractor.class);
        builder.addProperty("extractor", "thl-queue-extract", "storeName",
                "thl-queue");
        if (multiQueue)
            builder.addComponent("applier", "multi-queue-apply",
                    InMemoryMultiQueueApplier.class);
        else
            builder.addComponent("applier", "multi-queue-apply",
                    InMemoryTransactionalQueueApplier.class);
        builder.addProperty("applier", "multi-queue-apply", "storeName",
                "multi-queue");

        return builder.getConfig();
    }

    // Create an empty log directory or if the directory exists remove
    // any files within it.
    private File prepareLogDir(String logDirName) throws Exception
    {
        File logDir = new File(logDirName);
        // Delete old log if present.
        if (logDir.exists())
        {
            logger.info("Clearing log dir: " + logDir.getAbsolutePath());
            for (File f : logDir.listFiles())
            {
                f.delete();
            }
            logDir.delete();
        }

        // If the log directory exists now, we have a problem.
        if (logDir.exists())
        {
            throw new Exception(
                    "Unable to clear log directory, test cannot start: "
                            + logDir.getAbsolutePath());
        }

        // Create new log directory.
        logDir.mkdirs();
        return logDir;
    }

    // Returns a well-formed ReplDBMSEvent with a specified shard ID.
    private ReplDBMSEvent createEvent(long seqno, String shardId)
    {
        ArrayList<DBMSData> t = new ArrayList<DBMSData>();
        t.add(new StatementData("SELECT 1"));
        DBMSEvent dbmsEvent = new DBMSEvent(new Long(seqno).toString(), null,
                t, true, new Timestamp(System.currentTimeMillis()));
        ReplDBMSEvent replDbmsEvent = new ReplDBMSEvent(seqno, dbmsEvent);
        replDbmsEvent.getDBMSEvent().addMetadataOption(
                ReplOptionParams.SHARD_ID, shardId);
        return replDbmsEvent;
    }
}