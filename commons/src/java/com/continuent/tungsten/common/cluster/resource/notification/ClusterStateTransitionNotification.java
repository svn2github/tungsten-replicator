/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2009 Continuent Inc.
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
 * Initial developer(s): Edward Archibald
 * Contributor(s): ______________________.
 */

package com.continuent.tungsten.common.cluster.resource.notification;

import com.continuent.tungsten.common.cluster.resource.ResourceState;
import com.continuent.tungsten.common.cluster.resource.ResourceType;
import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.common.patterns.order.Sequence;

public class ClusterStateTransitionNotification
        extends ClusterResourceNotification
{

    private Sequence                    sequence;
    private ClusterStateTransitionPhase phase            = ClusterStateTransitionPhase.undefined;

    /**
     * 
     */
    private static final long           serialVersionUID = 1L;

    /**
     * @param clusterName
     * @param memberName TODO
     * @param notificationSource
     * @param resourceName
     * @param resourceState
     * @param resourceProps
     */
    public ClusterStateTransitionNotification(String clusterName,
            String memberName, String notificationSource, String resourceName,
            ResourceState resourceState, TungstenProperties resourceProps,
            ClusterStateTransitionPhase phase, Sequence sequence)
    {
        super(NotificationStreamID.MONITORING, clusterName, memberName,
                notificationSource, ResourceType.CONFIGURATION, resourceName,
                resourceState, resourceProps);

        this.phase = phase;
        this.sequence = sequence;
    }

    /**
     * Returns the sequence value.
     * 
     * @return Returns the sequence.
     */
    public Sequence getSequence()
    {
        return sequence;
    }

    /**
     * Sets the sequence value.
     * 
     * @param sequence The sequence to set.
     */
    public void setSequence(Sequence sequence)
    {
        this.sequence = sequence;
    }

    /**
     * Returns the phase value.
     * 
     * @return Returns the phase.
     */
    public ClusterStateTransitionPhase getPhase()
    {
        return phase;
    }

    /**
     * Sets the phase value.
     * 
     * @param phase The phase to set.
     */
    public void setPhase(ClusterStateTransitionPhase phase)
    {
        this.phase = phase;
    }

}
