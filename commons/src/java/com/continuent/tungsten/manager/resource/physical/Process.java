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

package com.continuent.tungsten.manager.resource.physical;

import java.util.concurrent.atomic.AtomicLong;

import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;

import com.continuent.tungsten.common.cluster.resource.Resource;
import com.continuent.tungsten.common.cluster.resource.ResourceType;
import com.continuent.tungsten.common.exception.ResourceException;

public class Process extends Resource
{
    /**
     * 
     */
    private static final long     serialVersionUID = 1L;
    private String                service          = null;
    private int                   port             = 0;
    private String                clusterName      = null;
    private String                member           = null;

    private JMXConnector          connection       = null;
    private MBeanServerConnection server           = null;

    private static AtomicLong     currentEpoch     = new AtomicLong(0);

    private long                  epoch            = -1L;

    public Process(String name) throws ResourceException
    {
        super(ResourceType.PROCESS, name);
        this.setService(name);
        this.childType = ResourceType.RESOURCE_MANAGER;
        this.isContainer = true;
        this.setEpoch(currentEpoch.incrementAndGet());
    }

    public String getMember()
    {
        return member;
    }

    /**
     * @return the port
     */
    public int getPort()
    {
        return port;
    }

    /**
     * @param port the port to set
     */
    public void setPort(int port)
    {
        this.port = port;
    }

    /**
     * @return the service
     */
    public String getService()
    {
        return service;
    }

    /**
     * @param service the service to set
     */
    public void setService(String service)
    {
        this.service = service;
    }

    /**
     * @param member the member name to set
     */
    public void setMember(String member)
    {
        this.member = member;
    }

    public void setConnection(JMXConnector connection)
    {
        this.connection = connection;
    }

    /**
     * Returns the server value.
     * 
     * @return Returns the server.
     */
    public MBeanServerConnection getServer()
    {
        return server;
    }

    /**
     * Sets the server value.
     * 
     * @param server The server to set.
     */
    public void setServer(MBeanServerConnection server)
    {
        this.server = server;
    }

    /**
     * Returns the connection value.
     * 
     * @return Returns the connection.
     */
    public JMXConnector getConnection()
    {
        return connection;
    }

    /**
     * Returns the epoch value.
     * 
     * @return Returns the epoch.
     */
    public long getEpoch()
    {
        return epoch;
    }

    /**
     * Sets the epoch value.
     * 
     * @param epoch The epoch to set.
     */
    public void setEpoch(long epoch)
    {
        this.epoch = epoch;
    }

    public String toString()
    {
        return String.format("/%s/%s/%s(%d)", getClusterName(), getMember(),
                getName(), getEpoch());
    }

    /**
     * Returns the clusterName value.
     * 
     * @return Returns the clusterName.
     */
    public String getClusterName()
    {
        return clusterName;
    }

    /**
     * Sets the clusterName value.
     * 
     * @param clusterName The clusterName to set.
     */
    public void setClusterName(String clusterName)
    {
        this.clusterName = clusterName;
    }

    @Override
    protected void finalize() throws Throwable
    {
        try
        {
            System.out.println(String.format("Finalize of %s", this));
        }
        catch (Throwable t)
        {
            throw t;
        }
    }

}
