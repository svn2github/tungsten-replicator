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

/**
 * Implements a simple harness to test filters that have minimal dependencies on
 * properties supplied the PluginContext class.
 */
public class FilterVerificationHelper
{
    // Filter to be tested.
    private Filter filter;

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
        filter.configure(null);
        filter.prepare(null);
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
        filter.release(null);
    }
}