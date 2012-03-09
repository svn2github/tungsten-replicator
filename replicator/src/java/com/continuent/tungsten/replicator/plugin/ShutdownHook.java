/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2012 Continuent Inc.
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

package com.continuent.tungsten.replicator.plugin;

import com.continuent.tungsten.replicator.ReplicatorException;

/**
 * This interface denotes a replicator plugin that has a shutdown method that
 * should be called to ensure the plugin will stop. For instance, this interface
 * can be used to break out of a blocking read on a socket by closing the socket
 * object.
 */
public interface ShutdownHook
{
    /**
     * Shut down component. This is called after the task interrupt and should
     * ensure the component responds correctly to an interrupt.
     * 
     * @throws ReplicatorException Thrown if shutdown is unsuccessful
     */
    public void shutdown(PluginContext context) throws ReplicatorException,
            InterruptedException;
}