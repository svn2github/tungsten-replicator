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
 * Contributor(s): Linas Virbalas
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

    private boolean                       addTruncateTable      = false;
    private long                          chunkSize             = -1;

    private int                           extractChannels       = 1;

    // Default queue size is set to 20.
    private int                           queueSize             = 20;

    private List<ParallelExtractorThread> threads;
    private ArrayBlockingQueue<DBMSEvent> queue;
    private ArrayBlockingQueue<Chunk>     chunks;

    private boolean                       threadsStarted        = false;

    private ChunksGeneratorThread         chunksGeneratorThread = null;
    private int                           activeThreads         = 0;
    private PluginContext                 context;
    private String                        chunkDefinitionFile   = null;

    private Hashtable<String, Long>       tableBlocks;

    protected String                      eventId               = null;

    /**
     * Sets the addTruncateTable value.
     * 
     * @param addTruncateTable The addTruncateTable to set.
     */
    public void setAddTruncateTable(boolean addTruncateTable)
    {
        this.addTruncateTable = addTruncateTable;
    }

    /**
     * Sets the chunkSize value.
     * 
     * @param chunkSize The chunkSize to set.
     */
    public void setChunkSize(long chunkSize)
    {
        this.chunkSize = chunkSize;
    }

    /**
     * Sets the extractChannels value.
     * 
     * @param extractChannels The extractChannels to set.
     */
    public void setExtractChannels(int extractChannels)
    {
        this.extractChannels = extractChannels;
    }

    /**
     * Sets the queueSize value.
     * 
     * @param queueSize The queueSize to set.
     */
    public void setQueueSize(int queueSize)
    {
        this.queueSize = queueSize;
    }

    /**
     * Sets the url value.
     * 
     * @param url The url to set.
     */
    public void setUrl(String url)
    {
        this.url = url;
    }

    /**
     * Sets the user value.
     * 
     * @param user The user to set.
     */
    public void setUser(String user)
    {
        this.user = user;
    }

    /**
     * Sets the password value.
     * 
     * @param password The password to set.
     */
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
                extractChannels, chunks, chunkDefinitionFile, chunkSize);

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
        this.eventId = eventId;
        chunksGeneratorThread.setEventId(eventId);
        for (int i = 0; i < extractChannels; i++)
        {
            threads.get(i).setEventId(eventId);
        }
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
        {
            if (addTruncateTable)
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
                        // If the number reaches 0, table was fully processed :
                        // no
                        // need to put tables back in there
                        tableBlocks.put(entry, blk - 1);
                }
                else
                {
                    // Issue 842 - do not hardcode schema name in SQL text.
                    // Instead, set it as default schema parameter.
                    StatementData sd = new StatementData("TRUNCATE TABLE "
                            + event.getMetadataOptionValue("table"), null,
                            event.getMetadataOptionValue("schema"));
                    event.getData().add(0, sd);

                    blk = Long
                            .valueOf(event.getMetadataOptionValue("nbBlocks"));
                    if (blk > 1)
                    {
                        tableBlocks.put(entry, blk - 1);
                    }
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

    /**
     * Sets the path to the chunk definition file.
     * 
     * @param chunkDefinitionFile Chunk definition file to use.
     */
    public void setChunkDefinitionFile(String chunkDefinitionFile)
    {
        this.chunkDefinitionFile = chunkDefinitionFile;
    }

}
