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

package com.continuent.tungsten.common.concurrent;

import java.util.concurrent.ThreadFactory;

/**
 * This class implements a thread factory used to create threads for the
 * SimpleThreadService class. The main thing the factory does is ensure threads
 * are properly named.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class SimpleThreadFactory implements ThreadFactory
{
    private final String  prefix;
    private volatile long threadCount = 0;

    /**
     * Creates a new thread factory with a prefix for thread names.
     * 
     * @param prefix Thread prefix
     */
    public SimpleThreadFactory(String prefix)
    {
        this.prefix = prefix;
    }

    /**
     * Creates a new thread with proper name. {@inheritDoc}
     * 
     * @see java.util.concurrent.ThreadFactory#newThread(java.lang.Runnable)
     */
    public Thread newThread(Runnable runnable)
    {
        String name = prefix + "-" + threadCount++;
        return new Thread(runnable, name);
    }
}