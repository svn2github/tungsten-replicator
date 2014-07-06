/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2013-2014 Continuent Inc.
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

package com.continuent.tungsten.replicator.scripting;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.concurrent.SimpleJobService;
import com.continuent.tungsten.replicator.ReplicatorException;

/**
 * Implements a service to invoke one or more script methods in separate threads
 * and then collect the responses.
 */
public class ScriptExecutorService
{
    private static Logger                              logger          = Logger.getLogger(ScriptExecutorService.class);
    private final String                               threadPrefix;
    private final List<ScriptExecutor>                 scriptExecutors;
    private final int                                  maxRequests;

    private final BlockingQueue<ScriptMethodRequest>   requests;
    private final BlockingQueue<ScriptMethodResponse>  responses;
    List<ScriptExecutorTaskStatus>                     taskStatusList;
    private AtomicInteger                              invocationCount = new AtomicInteger(
                                                                               0);

    private SimpleJobService<ScriptExecutorTaskStatus> jobService;

    /**
     * Create the script execution service.
     * 
     * @param threadPrefix Prefix for naming threads
     * @param scriptExecutors A set of script executors to use
     * @param maxRequests Maximum queue size for requests
     */
    public ScriptExecutorService(String threadPrefix,
            List<ScriptExecutor> scriptExecutors, int maxRequests)
    {
        this.threadPrefix = threadPrefix;
        this.scriptExecutors = scriptExecutors;
        this.maxRequests = maxRequests;
        this.requests = new ArrayBlockingQueue<ScriptMethodRequest>(maxRequests);
        this.responses = new ArrayBlockingQueue<ScriptMethodResponse>(
                maxRequests);
        this.taskStatusList = new ArrayList<ScriptExecutorTaskStatus>();
    }

    /**
     * Adds a request.
     */
    public void addRequest(ScriptMethodRequest request)
    {
        requests.add(request);
    }

    /**
     * Process the requests and clean up afterwards.
     * 
     * @return True if tasks complete cleanly or false if there is some kind of
     *         error
     */
    public boolean process() throws InterruptedException, ReplicatorException
    {
        // Start the service and load the tasks.
        jobService = new SimpleJobService<ScriptExecutorTaskStatus>(
                threadPrefix, scriptExecutors.size(), maxRequests, 10);
        List<Future<ScriptExecutorTaskStatus>> taskFutures = new ArrayList<Future<ScriptExecutorTaskStatus>>();
        for (ScriptExecutor exec : scriptExecutors)
        {
            ScriptExecutorTask execTask = new ScriptExecutorTask(exec,
                    requests, responses);
            taskFutures.add(jobService.submit(execTask));
        }

        // Wait for tasks to finish or report an error.
        for (Future<ScriptExecutorTaskStatus> future : taskFutures)
        {
            ScriptExecutorTaskStatus status;
            try
            {
                status = future.get();
                this.taskStatusList.add(status);
            }
            catch (ExecutionException e)
            {
                jobService.shutdownNow();
                throw new ReplicatorException(
                        "Failure when checking status of script executor: message="
                                + e.getMessage(), e);
            }

            if (status.isSuccessful())
            {
                invocationCount.addAndGet(status.getCount());
                if (logger.isDebugEnabled())
                {
                    logger.debug("Executor task completed successfully: count="
                            + status.getCount());
                }
            }
            else
            {
                // If we have an error, we need to shut down.
                jobService.shutdownNow();
                return false;
            }
        }

        // If we get here everything should be good.
        return true;
    }

    /**
     * Return all executor task statuses received.
     */
    public List<ScriptExecutorTaskStatus> getTaskStatusList()
    {
        return this.taskStatusList;
    }

    /**
     * Returns the total number of background invocations reported by background
     * tasks.
     */
    public int getMethodInvocationCount()
    {
        return invocationCount.get();
    }

    /**
     * Return all responses received.
     */
    public List<ScriptMethodResponse> getMethodResponses()
    {
        LinkedList<ScriptMethodResponse> list = new LinkedList<ScriptMethodResponse>();
        responses.drainTo(list);
        return list;
    }

    /**
     * Shut down the job service by letting current tasks run to completion but
     * accepting no further tasks.
     */
    public void shutdown()
    {
        jobService.shutdown();
    }

    /**
     * Shut down the job service immediately. This is used to handle an error.
     */
    public void shutdownNow()
    {
        jobService.shutdownNow();
    }
}