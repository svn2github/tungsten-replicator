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

import java.util.List;

import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * Partitions event by assigning to the least loaded queue.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class LoadBalancingPartitioner implements StatefulPartitioner
{
    private List<PartitionMetadata> partitionList;

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.storage.parallel.Partitioner#setPartitions(int)
     */
    public synchronized void setPartitions(int availablePartitions)
    {
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.storage.parallel.Partitioner#setPartitionMetadata(java.util.List)
     */
    public void setPartitionMetadata(List<PartitionMetadata> partitions)
    {
        partitionList = partitions;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.storage.parallel.Partitioner#setContext(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void setContext(PluginContext context)
    {
    }

    /**
     * Returns the partition with the smallest current size or the first
     * partition that has a current size of zero.
     * 
     * @see com.continuent.tungsten.replicator.storage.parallel.Partitioner#partition(com.continuent.tungsten.replicator.event.ReplDBMSEvent,
     *      int, int)
     */
    public synchronized PartitionerResponse partition(ReplDBMSHeader event,
            int taskId)
    {
        long minSize = Long.MAX_VALUE;
        int partition = 0;
        for (PartitionMetadata meta : partitionList)
        {
            long size = meta.getCurrentSize();
            if (size == 0)
            {
                partition = meta.getPartitionNumber();
                break;
            }
            else if (size < minSize)
            {
                minSize = size;
                partition = meta.getPartitionNumber();
            }
        }

        return new PartitionerResponse(partition, false);
    }
}