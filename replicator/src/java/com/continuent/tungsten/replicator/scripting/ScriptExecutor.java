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

import java.util.Map;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.plugin.ReplicatorPlugin;

/**
 * Denotes a class capable of executing a batch load script. This interface
 * conforms to conventions for replicator plugins.
 */
public interface ScriptExecutor extends ReplicatorPlugin
{
    /** Sets the script name. */
    public void setScript(String script);

    /** Returns the script name. */
    public String getScript();

    /** Sets the default data source name, if it exists. */
    public void setDefaultDataSourceName(String name);

    /** Sets a map of objects to be inserted into the executor context. */
    public void setContextMap(Map<String, Object> contextMap);

    /**
     * Register a method name. This must be called prior to invoking any
     * individual script method.
     * 
     * @param method Name of the method in the script
     * @return True if the method is found and registered, otherwise false
     * @throws ReplicatorException Thrown if registration fails
     */
    public boolean register(String method) throws ReplicatorException;

    /**
     * Executes a registered script method including a single optional argument.
     * 
     * @param method Name of the method in the script
     * @param argument Argument to pass in during method invocation
     * @return An object or null if the method does not return a value
     * @throws ReplicatorException Thrown if execute operation fails
     */
    public Object execute(String method, Object argument)
            throws ReplicatorException;
}