/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2013 Continuent Inc.
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
 * Contributor(s): Robert Hodges
 */

package com.continuent.tungsten.common.cluster.resource;

/**
 * Stores information about a cluster member that is helpful in deciding whether
 * a group of members has quorum. To prevent construction of inconsistent member
 * sets the constructor and setter methods are package protected.
 */
public class ClusterMember
{
    private String  name       = null;
    private boolean configured = false;
    private boolean inView     = false;
    private boolean witness    = false;
    private boolean validated  = false;
    private boolean reachable  = false;

    /**
     * Instantiate a new member record.
     * 
     * @param name The member name. Names must be unique within a group.
     */
    ClusterMember(String name)
    {
        this.name = name;
    }

    /** Returns the member name. */
    public String getName()
    {
        return name;
    }

    /**
     * Returns true if the member belongs to the external configuration.
     */
    public boolean isConfigured()
    {
        return configured;
    }

    /**
     * Specifies if this member is externally configured to be a member of the
     * group, regardless whether it participates in the view or not.
     */
    void setConfigured(boolean configured)
    {
        this.configured = configured;
    }

    /** Returns true if the member is in the current group view. */
    public boolean isInView()
    {
        return inView;
    }

    /**
     * Specifies whether this member is in the current group view. Members may
     * be in the group view even if they are not externally configured as
     * cluster members.
     */
    void setInView(boolean inView)
    {
        this.inView = inView;
    }

    /** Returns true if this member is actually a witness host. */
    public boolean isWitness()
    {
        return witness;
    }

    /** Specifies whether this member is a witness. */
    void setWitness(boolean witness)
    {
        this.witness = witness;
    }

    /** Returns true if this member has been validated by pinging through GC. */
    public Boolean getValidated()
    {
        return validated;
    }

    /** Specifies whether this member has been validated by pinging through GC. */
    void setValidated(Boolean validated)
    {
        this.validated = validated;
    }

    /**
     * Returns true if this member is reachable using a ping command over the
     * network.
     */
    public Boolean getReachable()
    {
        return reachable;
    }

    /**
     * Specifies whether this member is reachable using a ping command over the
     * network.
     */
    void setReachable(Boolean reachable)
    {
        this.reachable = reachable;
    }
}