/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2013 Continuent Inc.
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

package com.continuent.tungsten.replicator.filter;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * Implements a simple harness to test filters that have minimal dependencies on
 * properties supplied the PluginContext class. Where such dependencies exist
 * users are responsible for supplying a plugin that has suitable properties.
 */
public class FilterVerificationHelper
{
    // Filter to be tested.
    private Filter        filter;

    // Plugin context, which is usually a ReplicatorRuntime.
    private PluginContext context;

    /**
     * Set the replication context, which will be used for life-cycle calls to
     * filter. This is optional. For filters that do not use PluginContext it is
     * fine for this to be null.
     */
    public void setContext(PluginContext context)
    {
        this.context = context;
    }

    /**
     * Assign a filter to be tested. Caller must instantiate and assign
     * properties, then call this method. The help calls configure() and
     * prepare() on the filter instance.
     * 
     * @throws InterruptedException
     * @throws ReplicatorException
     */
    public void setFilter(Filter filter) throws ReplicatorException,
            InterruptedException
    {
        this.filter = filter;
        filter.configure(context);
        filter.prepare(context);
    }

    /**
     * Deliver a transaction to the filter and return the filter output.
     * 
     * @throws InterruptedException
     * @throws ReplicatorException
     */
    public ReplDBMSEvent filter(ReplDBMSEvent event)
            throws ReplicatorException, InterruptedException
    {
        return filter.filter(event);
    }

    /**
     * Calls release() method on the filter.
     * 
     * @throws InterruptedException
     * @throws ReplicatorException
     */
    public void done() throws ReplicatorException, InterruptedException
    {
        filter.release(context);
    }
}