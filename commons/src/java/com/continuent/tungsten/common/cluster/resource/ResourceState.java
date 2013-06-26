
package com.continuent.tungsten.common.cluster.resource;

import java.text.MessageFormat;

import com.continuent.tungsten.common.exception.ResourceException;

public enum ResourceState
{
    ONLINE, OFFLINE, SYNCHRONIZING, JOINING, SUSPECT, STOPPED, UNKNOWN, TIMEOUT, DEGRADED, SHUNNED, CONSISTENT, MODIFIED, FAILED, BACKUP, UNREACHABLE, EXTENSION, RESTORING, RUNNING;

    /**
     * Create a ResourceState from a non case sensitive String
     * 
     * @param x the string to be converted
     * @return the converted ResourceState
     * @throws ResourceException if could not cast string to a ResourceState
     */
    public static ResourceState fromString(String x) throws ResourceException
    {
        for (ResourceState currentType : ResourceState.values())
        {
            if (x.equalsIgnoreCase(currentType.toString()))
                return currentType;
        }
        throw new ResourceException(MessageFormat.format(
                "Cannot cast into a known ResourceState: {0}", x));
    }
}
