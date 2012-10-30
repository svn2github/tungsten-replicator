
package com.continuent.tungsten.common.cluster.resource;

import java.io.Serializable;

public class RemoteDataService extends Resource implements Serializable
{

    /**
     * 
     */
    private static final long serialVersionUID = -4287212613595722268L;

    /*
     * State transitions for RemoteDataService:
     */
    // UNKNOWN->ONLINE->CONSISTENT
    // UNKNOWN->UNREACHABLE->OFFLINE

    private ResourceState     resourceState    = ResourceState.UNKNOWN;

    public RemoteDataService()
    {
        // TODO Auto-generated constructor stub
    }

    public RemoteDataService(ResourceType type, String name, ResourceState state)
    {
        super(type, name);
        setResourceState(state);

    }

    /**
     * Returns the resourceState value.
     * 
     * @return Returns the resourceState.
     */
    public ResourceState getResourceState()
    {
        return resourceState;
    }

    /**
     * Sets the resourceState value.
     * 
     * @param resourceState The resourceState to set.
     */
    public void setResourceState(ResourceState resourceState)
    {
        this.resourceState = resourceState;
    }

    public String toString()
    {
        return String.format("%s: name=%s, resourceState=%s", getType(),
                getName(), getResourceState());
    }
}
