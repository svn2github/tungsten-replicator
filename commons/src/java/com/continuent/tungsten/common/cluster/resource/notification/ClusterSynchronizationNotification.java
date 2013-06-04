package com.continuent.tungsten.common.cluster.resource.notification;

import com.continuent.tungsten.common.cluster.resource.ResourceState;
import com.continuent.tungsten.common.cluster.resource.ResourceType;
import com.continuent.tungsten.common.config.TungstenProperties;

public class ClusterSynchronizationNotification
        extends ClusterResourceNotification
{

    /**
     * 
     */
    private static final long serialVersionUID = -8510022412295723387L;

    public ClusterSynchronizationNotification(String clusterName,
            String memberName, String notificationSource,
            ResourceType resourceType, String resourceName,
            ResourceState resourceState, TungstenProperties resourceProps)
    {
        super(clusterName, memberName, notificationSource, resourceType,
                resourceName, resourceState, resourceProps);
        // TODO Auto-generated constructor stub
    }

    public ClusterSynchronizationNotification(NotificationStreamID streamID,
            String clusterName, String memberName, String notificationSource,
            ResourceType resourceType, String resourceName,
            ResourceState resourceState, TungstenProperties resourceProps)
    {
        super(streamID, clusterName, memberName, notificationSource,
                resourceType, resourceName, resourceState, resourceProps);
        // TODO Auto-generated constructor stub
    }

}
