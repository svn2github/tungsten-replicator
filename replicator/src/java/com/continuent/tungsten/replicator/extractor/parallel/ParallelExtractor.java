/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2014 Continuent Inc.
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
 * Initial developer(s): Stephane Giron
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.extractor.parallel;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.InSequenceNotification;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.event.DBMSEmptyEvent;
import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.extractor.RawExtractor;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class ParallelExtractor implements RawExtractor
{
    private static Logger                 logger                = Logger.getLogger(ParallelExtractor.class);

    private String                        url                   = null;
    private String                        user                  = "root";
    private String                        password              = "rootpass";

    private int                           extractChannels       = 5;
    private List<ParallelExtractorThread> threads;
    private int                           queueSize             = 20;

    private ArrayBlockingQueue<DBMSEvent> queue;
    private ArrayBlockingQueue<Chunk>     chunks;

    private boolean                       threadsStarted        = false;

    private ChunksGeneratorThread         chunksGeneratorThread = null;
    private int                           activeThreads         = 0;
    private PluginContext                 context;
    private String                        chunkDefinitionFile   = null;

    private Hashtable<String, Long>       tableBlocks;

    public void setUrl(String url)
    {
        this.url = url;
    }

    public void setUser(String user)
    {
        this.user = user;
    }

    public void setPassword(String password)
    {
        this.password = password;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    @Override
    public void configure(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        if (chunkDefinitionFile == null)
            logger.info("No chunk definition file provided. Scanning whole database.");

    }

    /**
     * {@inheritDoc}
     * 
     * @throws Exception
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    @Override
    public void prepare(PluginContext context) throws ReplicatorException
    {
        this.context = context;

        chunks = new ArrayBlockingQueue<Chunk>(5 * extractChannels);

        queue = new ArrayBlockingQueue<DBMSEvent>(queueSize);

        chunksGeneratorThread = new ChunksGeneratorThread(user, url, password,
                extractChannels, chunks, chunkDefinitionFile);

        tableBlocks = new Hashtable<String, Long>();

        threads = new ArrayList<ParallelExtractorThread>();
        for (int i = 0; i < extractChannels; i++)
        {
            // Create extractor threads
            ParallelExtractorThread extractorThread = new ParallelExtractorThread(
                    url, user, password, chunks, queue);
            extractorThread.setName("ParallelExtractorThread-" + i);
            activeThreads++;
            threads.add(extractorThread);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    @Override
    public void release(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.RawExtractor#setLastEventId(java.lang.String)
     */
    @Override
    public void setLastEventId(String eventId) throws ReplicatorException
    {
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.RawExtractor#extract()
     */
    @Override
    public DBMSEvent extract() throws ReplicatorException, InterruptedException
    {
        if (!threadsStarted)
        {
            chunksGeneratorThread.start();

            for (Iterator<ParallelExtractorThread> iterator = threads
                    .iterator(); iterator.hasNext();)
            {
                iterator.next().start();
            }

            threadsStarted = true;
        }

        DBMSEvent event = queue.take();
        if (event instanceof DBMSEmptyEvent)
        {
            activeThreads--;
            if (activeThreads == 0)
            {
                // Job is now complete. Check whether we can go back to offline
                // state
                context.getEventDispatcher().put(new InSequenceNotification());
            }
            return null;
        }
        else
        // TODO : this should be done only if addTruncateTable setting is set.
        {
            // Check metadata of the event
            String entry = event.getMetadataOptionValue("schema") + "."
                    + event.getMetadataOptionValue("table");

            Long blk = tableBlocks.remove(entry);
            if (blk != null)
            {
                // Table already in there... no need to add TRUNCATE, but
                // decrement number of remaining blocks
                if (blk > 1)
                    // If the number reaches 0, table was fully processed : no
                    // need to put tables back in there
                    tableBlocks.put(entry, blk - 1);
            }
            else
            {
                event.getData().add(0,
                        new StatementData("TRUNCATE TABLE " + entry));

                blk = Long.valueOf(event.getMetadataOptionValue("nbBlocks"));
                if (blk > 1)
                {
                    tableBlocks.put(entry, blk - 1);
                }

            }
        }

        return event;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.RawExtractor#extract(java.lang.String)
     */
    @Override
    public DBMSEvent extract(String eventId) throws ReplicatorException,
            InterruptedException
    {
        return null;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.RawExtractor#getCurrentResourceEventId()
     */
    @Override
    public String getCurrentResourceEventId() throws ReplicatorException,
            InterruptedException
    {
        return null;
    }

    public void setChunkDefinitionFile(String chunkDefinitionFile)
    {
        this.chunkDefinitionFile = chunkDefinitionFile;
    }

}
