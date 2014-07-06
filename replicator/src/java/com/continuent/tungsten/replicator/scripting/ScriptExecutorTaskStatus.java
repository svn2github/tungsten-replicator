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

/**
 * Provides a response from a task executor that can be used to assess whether
 * the task completed successfully.
 */
public class ScriptExecutorTaskStatus
{
    private final int                  count;
    private final boolean              successful;
    private final ScriptMethodResponse failedResponse;

    /**
     * Creates a new <code>ScriptExecutorTaskResponse</code> object
     * 
     * @param count Count of requests processed
     * @param successful If true, all requests were successful
     * @param failedResponse Filled in with failing response if the last request
     *            was unsuccessful
     */
    public ScriptExecutorTaskStatus(int count, boolean successful,
            ScriptMethodResponse failedResponse)
    {
        this.count = count;
        this.successful = successful;
        this.failedResponse = failedResponse;
    }

    public int getCount()
    {
        return count;
    }

    public boolean isSuccessful()
    {
        return successful;
    }

    public ScriptMethodResponse getFailedResponse()
    {
        return failedResponse;
    }
}