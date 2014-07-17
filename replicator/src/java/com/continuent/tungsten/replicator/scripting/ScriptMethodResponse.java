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
 * Initial developer(s): Robert Hodges
 * Contributor(s): 
 */

package com.continuent.tungsten.replicator.scripting;

/**
 * Implements a response from script method execution. This includes the
 * original request as well as the return value if successful or the throwable
 * if not.
 */
public class ScriptMethodResponse
{
    private final ScriptMethodRequest request;
    private final Object              value;
    private final Throwable           throwable;
    private final boolean             successful;

    /**
     * Creates a new script method invocation response.
     * 
     * @param request The request to which this response applies
     * @param value The return value or null if there is no return value
     * @param throwable The exception resulting from a failed execution
     * @param successful If true the invocation completed normally, otherwise
     *            false in which case we also return the exception
     */
    public ScriptMethodResponse(ScriptMethodRequest request, Object value,
            Throwable throwable, boolean successful)
    {
        this.request = request;
        this.value = value;
        this.throwable = throwable;
        this.successful = successful;
    }

    public ScriptMethodRequest getRequest()
    {
        return request;
    }

    public Object getValue()
    {
        return value;
    }

    public Throwable getThrowable()
    {
        return throwable;
    }

    public boolean isSuccessful()
    {
        return successful;
    }
}