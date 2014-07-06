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
 * Implements a request to execute a script method.
 */
public class ScriptMethodRequest
{
    private final String method;
    private final Object argument;

    /**
     * Creates a new method invocation request.
     * 
     * @param method The name of the method we want to execute
     * @param argument An argument to that method or null if there is none
     */
    public ScriptMethodRequest(String method, Object argument)
    {
        this.method = method;
        this.argument = argument;
    }

    public String getMethod()
    {
        return method;
    }

    public Object getArgument()
    {
        return argument;
    }
}