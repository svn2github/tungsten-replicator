
package com.continuent.tungsten.common.directory;

import java.io.Serializable;

public class DirectorySession implements Serializable
{
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    Directory                 parent           = null;
    String                    sessionID        = null;
    ResourceNode              currentNode      = null;
    private long              timeCreated      = 0L;
    private long              lastTimeAccessed = 0L;

    public DirectorySession(Directory parent, String sessionID,
            ResourceNode currentNode)
    {
        this.parent = parent;
        this.sessionID = sessionID;
        this.currentNode = currentNode;
        this.timeCreated = System.currentTimeMillis();
        this.lastTimeAccessed = this.timeCreated;
    }

    /**
     * @return the parent
     */
    public Directory getParent()
    {
        return parent;
    }

    /**
     * @param parent the parent to set
     */
    public void setParent(Directory parent)
    {
        this.parent = parent;
    }

    /**
     * @return the sessionID
     */
    public String getSessionID()
    {
        return sessionID;
    }

    /**
     * @param sessionID the sessionID to set
     */
    public void setSessionID(String sessionID)
    {
        this.sessionID = sessionID;
    }

    /**
     * @return the currentNode
     */
    public ResourceNode getCurrentNode()
    {
        return currentNode;
    }

    /**
     * @param currentNode the currentNode to set
     */
    public void setCurrentNode(ResourceNode currentNode)
    {
        this.currentNode = currentNode;
    }

    public String toString()
    {
        return "sessionID=" + getSessionID();
    }

    public long getTimeCreated()
    {
        return timeCreated;
    }

    public void setTimeCreated(long timeCreated)
    {
        this.timeCreated = timeCreated;
    }

    public long getLastTimeAccessed()
    {
        return lastTimeAccessed;
    }

    public void setLastTimeAccessed(long lastTimeAccessed)
    {
        this.lastTimeAccessed = lastTimeAccessed;
    }

}
