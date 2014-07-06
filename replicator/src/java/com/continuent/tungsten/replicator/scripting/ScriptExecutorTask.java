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
 * Contributor(s): Linas Virbalas
 */

package com.continuent.tungsten.replicator.scripting;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

/**
 * Implements a wrapper around ScriptExecutor to process method invocations from
 * a request queue and return the result on a response queue. The wrapper
 * terminates when the request queue is empty.
 */
public class ScriptExecutorTask implements Callable<ScriptExecutorTaskStatus>
{
    private static Logger                             logger = Logger.getLogger(ScriptExecutorTask.class);
    private final ScriptExecutor                      executor;
    private final BlockingQueue<ScriptMethodRequest>  requests;
    private final BlockingQueue<ScriptMethodResponse> responses;

    private AtomicInteger                             count  = new AtomicInteger(
                                                                     0);

    /**
     * Create a new batch method executor.
     * 
     * @param executor Executor we want to wrap
     * @param requests Request queue
     * @param responses Response queue
     */
    public ScriptExecutorTask(ScriptExecutor executor,
            BlockingQueue<ScriptMethodRequest> requests,
            BlockingQueue<ScriptMethodResponse> responses)
    {
        this.executor = executor;
        this.requests = requests;
        this.responses = responses;
    }

    /** Return the integer value of the count. */
    public int getCount()
    {
        return count.get();
    }

    /**
     * Processes method execution requests until the request queue is empty.
     * Following normal end return the count of processed request.
     */
    public ScriptExecutorTaskStatus call() throws Exception
    {
        ScriptMethodRequest request;
        while ((request = requests.poll()) != null)
        {
            ScriptMethodResponse response = null;
            try
            {
                String method = request.getMethod();
                Object argument = request.getArgument();
                Object value = executor.execute(method, argument);
                response = new ScriptMethodResponse(request, value, null, true);
            }
            catch (Throwable t)
            {
                // If we had a failure, we should log the error and exit.
                response = new ScriptMethodResponse(request, null, t, false);
                String msg = formatErrorMessage(request);
                logger.error(msg, response.getThrowable());
                ScriptExecutorTaskStatus status = new ScriptExecutorTaskStatus(
                        count.get(), false, response);
                return status;
            }
            finally
            {
                responses.add(response);
            }

            // Increment the count of successfully processed requests.
            count.incrementAndGet();
        }

        // If we get this far we were successful. Return the status to indicate
        // this.
        return new ScriptExecutorTaskStatus(count.get(), true, null);
    }

    // Format an error message when returning following a failure.
    private String formatErrorMessage(ScriptMethodRequest request)
    {
        return String
                .format("Unexpected script execution failure: script=%s method=%s argument=%s",
                        executor.getScript(), request.getMethod(), request
                                .getArgument().toString());
    }
}