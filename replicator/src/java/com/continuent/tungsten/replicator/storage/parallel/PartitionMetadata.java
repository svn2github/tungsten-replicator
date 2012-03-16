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

package com.continuent.tungsten.replicator.storage.parallel;

/**
 * Denotes information describing a partition. This information is available to
 * partitioners so that they can assign based on current state of partition
 * themselves (i.e. parallel queues). This allows support for load balancing and
 * other context sensitive decisions.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public interface PartitionMetadata
{
    /**
     * Returns the number of the partition to which the metadata applies.
     */
    public int getPartitionNumber();

    /**
     * Returns the number of events currently in the partition. The
     * implementation must be non-blocking and thread-safe. This number may be
     * an estimate.
     */
    public long getCurrentSize();
}