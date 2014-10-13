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

import com.continuent.tungsten.common.cluster.resource.DataServer;
import com.continuent.tungsten.common.cluster.resource.ResourceState;
import com.continuent.tungsten.common.cluster.resource.ResourceType;
import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.common.config.cluster.DataServerConditionMapping;
import com.continuent.tungsten.common.config.cluster.MappedAction;

public class DataServerNotification extends ClusterResourceNotification
{
    /**
     * 
     */
    private static final long          serialVersionUID = 1L;

    private Exception                  lastException;

    private boolean                    readOnly         = false;

    private DataServerConditionMapping conditionMapping = null;

    public DataServerNotification(String clusterName, String memberName,
            String resourceName, ResourceState resourceState, String source,
            DataServer dataServer, TungstenProperties dsQueryResultProps)
    {
        super(NotificationStreamID.MONITORING, clusterName, memberName, source,
                ResourceType.DATASERVER, resourceName, resourceState, null);
        setResourceProps(dsQueryResultProps);
        setResource(dataServer);
    }

    public void setResourceProps(TungstenProperties resourceProps)
    {
        super.setResourceProps(resourceProps);
        if (resourceProps != null)
        {
            if (resourceProps.getInt("is_read_only", "0", false) == 1)
            {
                setReadOnly(true);
            }
            else
            {
                setReadOnly(false);
            }
        }

    }

    public DataServer getDataServer()
    {
        return (DataServer) getResource();
    }

    public Exception getLastException()
    {
        return lastException;
    }

    public void setLastException(Exception lastException)
    {
        this.lastException = lastException;
    }

    public String display()
    {
        TungstenProperties dsQueryProps = getResourceProps();

        if (dsQueryProps == null)
        {
            return super.toString();
        }

        StringBuilder builder = new StringBuilder();
        builder.append(super.toString()).append("\n");
        builder.append("Query results:\n");
        builder.append(String.format("%-30s %-30s\n", "COLUMN", "VALUE"));
        builder.append(String.format("%-30s %-30s\n",
                "------------------------------",
                "------------------------------"));

        for (Object key : dsQueryProps.getProperties().keySet())
        {
            builder.append(String.format("%-30s %-30s\n", key.toString(),
                    dsQueryProps.getString(key.toString())));
        }

        return builder.toString();

    }

    public void setReadOnly(boolean readOnly)
    {
        this.readOnly = readOnly;
    }

    public boolean isReadOnly()
    {
        return readOnly;
    }

    public DataServerConditionMapping getConditionMapping()
    {
        return conditionMapping;
    }

    public void setConditionMapping(DataServerConditionMapping conditionMapping)
    {
        this.conditionMapping = conditionMapping;
    }

    public MappedAction getAction()
    {
        if (conditionMapping != null)
        {
            return conditionMapping.getAction();
        }

        return MappedAction.NONE;
    }

    public ResourceState getResourceState()
    {
        if (conditionMapping != null)
        {
            return conditionMapping.getState();
        }

        return ResourceState.UNKNOWN;
    }
}
