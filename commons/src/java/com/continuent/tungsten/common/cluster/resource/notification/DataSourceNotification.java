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

import com.continuent.tungsten.common.cluster.resource.DataSource;
import com.continuent.tungsten.common.cluster.resource.DataSourceRole;
import com.continuent.tungsten.common.cluster.resource.ResourceState;
import com.continuent.tungsten.common.cluster.resource.ResourceType;
import com.continuent.tungsten.common.config.TungstenProperties;

public class DataSourceNotification extends ClusterResourceNotification
{
    protected DataSourceRole  role             = DataSourceRole.undefined;
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public DataSourceNotification(String clusterName, String memberName,
            String resourceName, ResourceState state, String source,
            TungstenProperties resourceProps)
    {
        super(NotificationStreamID.MONITORING, clusterName, memberName, source,
                ResourceType.DATASOURCE, resourceName, state, resourceProps);

    }

    public DataSourceNotification(NotificationStreamID streamID,
            String clusterName, String memberName, String resourceName,
            ResourceState state, String source, TungstenProperties resourceProps)
    {
        super(streamID, clusterName, memberName, source,
                ResourceType.DATASOURCE, resourceName, state, resourceProps);
        if (resourceProps != null)
        {
            String dsRole = resourceProps.getString(DataSource.ROLE);
            if (dsRole != null)
            {
                this.role = DataSourceRole.valueOf(dsRole);
            }
        }
    }

    public DataSourceNotification(NotificationStreamID streamID, String source,
            DataSource ds)
    {
        super(streamID, ds.getDataServiceName(), ds.getName(), source,
                ResourceType.DATASOURCE, ds.getName(), ds.getState(), ds
                        .toProperties());
        this.role = ds.getDataSourceRole();
    }

    public DataSource getDataSource()
    {
        return (DataSource) new DataSource(resourceProps);
    }

    public DataSourceRole getRole()
    {
        return role;
    }
}
