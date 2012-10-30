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

import com.continuent.tungsten.common.cluster.resource.Replicator;
import com.continuent.tungsten.common.cluster.resource.ResourceState;
import com.continuent.tungsten.common.cluster.resource.ResourceType;
import com.continuent.tungsten.common.config.TungstenProperties;

public class ReplicatorNotification extends ClusterResourceNotification
{

    /**
     * Used to determine if a given de-serialized object is compatible with this
     * class version.<br>
     * This value must be changed if and only if the new version of this class
     * is not compatible with old versions. See <a
     * href=http://java.sun.com/products/jdk/1.1/docs/guide
     * /serialization/spec/version.doc.html/> for a list of compatible changes.
     */
    private static final long   serialVersionUID               = -2097111546144612171L;

    private static final String REPLICATOR_STATE_ONLINE        = "ONLINE";
    private static final String REPLICATOR_STATE_OFFLINE       = "OFFLINE";
    private static final String REPLICATOR_STATE_ERROR         = "OFFLINE:ERROR";
    private static final String REPLICATOR_STATE_BACKUP        = "OFFLINE:BACKUP";
    private static final String REPLICATOR_STATE_RESTORING     = "OFFLINE:RESTORING";
    private static final String REPLICATOR_STATE_SYNCHRONIZING = "SYNCHRONIZING";

    /**
     * @param clusterName
     * @param memberName TODO
     * @param notificationSource
     * @param resourceName
     * @param resourceState
     * @param resourceProps
     */
    public ReplicatorNotification(String clusterName, String memberName,
            String notificationSource, String resourceName,
            ResourceState resourceState, TungstenProperties resourceProps)
    {
        super(NotificationStreamID.MONITORING, clusterName, memberName,
                notificationSource, ResourceType.REPLICATOR, resourceName,
                resourceState, resourceProps);
    }

    /**
     * Parses the given state provided by the replicator and guess a generic
     * resourceState from it
     * 
     * @param state the state of the connected replicator, as provided by its
     *            JMX state attribute
     * @return a resource state, one of {@link ResourceState}
     */
    static public ResourceState replicatorStateToResourceState(String state)
    {
        if (state.startsWith(REPLICATOR_STATE_ONLINE))
            return ResourceState.ONLINE;
        else if (state.startsWith(REPLICATOR_STATE_OFFLINE))
        {
            if (state.equals(REPLICATOR_STATE_ERROR))
            {
                return ResourceState.SUSPECT;
            }
            else if (state.equals(REPLICATOR_STATE_BACKUP))
            {
                return ResourceState.BACKUP;
            }
            else if (state.equals(REPLICATOR_STATE_RESTORING))
            {
                return ResourceState.RESTORING;
            }
            else
            {
                return ResourceState.OFFLINE;
            }
        }
        else if (state.contains(REPLICATOR_STATE_SYNCHRONIZING))
            return ResourceState.SYNCHRONIZING;
        else
            return ResourceState.UNKNOWN;
    }

    public String getRole()
    {
        return resourceProps.getString(Replicator.ROLE);
    }

    public String getMasterReplicator()
    {
        String masterUri = resourceProps
                .getString(Replicator.MASTER_CONNECT_URI);
        return masterUri.substring(masterUri.indexOf("//") + 2,
                masterUri.lastIndexOf("/"));
    }
}
