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

import java.text.SimpleDateFormat;
import java.util.TimeZone;

/**
 * Implements a value partitioner that always returns an empty string.
 */
public class DateTimeValuePartitioner implements ValuePartitioner
{
    SimpleDateFormat formatter = new SimpleDateFormat();

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.batch.ValuePartitioner#setFormat(java.lang.String)
     */
    public void setFormat(String format)
    {
        formatter.applyPattern(format);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.batch.ValuePartitioner#setTimeZone(java.util.TimeZone)
     */
    public void setTimeZone(TimeZone tz)
    {
        formatter.setTimeZone(tz);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.batch.ValuePartitioner#partition(java.lang.Object)
     */
    public String partition(Object value)
    {
        return formatter.format(value);
    }
}
