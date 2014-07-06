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

package com.continuent.tungsten.replicator.applier.batch;

import java.util.TimeZone;

/**
 * Denotes a file that partitions a data value based on a key generated from an
 * argument passed to the partition method.
 */
public interface ValuePartitioner
{
    /** Format string for partitioner set in configuration file. */
    public void setFormat(String format);

    /** Time zone set in configuration file. */
    public void setTimeZone(TimeZone tz);

    /** Return a key that can be used to group data. */
    public String partition(Object value);
}
