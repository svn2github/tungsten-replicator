/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2013-2014 Continuent Inc.
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
 * Initial developer(s): Robert Hodges
 * Contributor(s): 
 */

package com.continuent.tungsten.replicator.datasource;

import java.util.List;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.plugin.PluginContext;
import com.continuent.tungsten.replicator.service.PipelineService;

/**
 * Provides a service interface interface to data source implementations. This
 * service is responsible for initializing implementations as well as underlying
 * catalog data at pipeline start-up time.
 */
public class DataSourceService implements PipelineService
{
    // Properties.
    private String            name;

    // Operational variables.
    private DataSourceManager manager = new DataSourceManager();

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.service.PipelineService#getName()
     */
    public String getName()
    {
        return name;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.service.PipelineService#setName(java.lang.String)
     */
    public void setName(String name)
    {
        this.name = name;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        // Find names of data sources.
        TungstenProperties replicatorProps = context.getReplicatorProperties();
        List<String> datasourceNames = replicatorProps
                .getStringList("replicator.datasources");

        // Instantiate and configure each data source.
        for (String name : datasourceNames)
        {
            String dsPrefix = "replicator.datasource." + name;
            String className = replicatorProps.get(dsPrefix);
            TungstenProperties attributes = replicatorProps.subset(dsPrefix
                    + ".", true);
            attributes.setBeanSupportEnabled(true);
            UniversalDataSource ds = manager.add(name, className, attributes);
            ds.configure();
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        // Instantiate and configure each data source.
        for (String name : manager.names())
        {
            UniversalDataSource ds = manager.find(name);
            ds.prepare();
            ds.initialize();
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    @Override
    public void release(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        manager.removeAndReleaseAll();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.service.PipelineService#status()
     */
    public TungstenProperties status()
    {
        TungstenProperties props = new TungstenProperties();
        for (String name : manager.names())
        {
            UniversalDataSource ds = manager.find(name);
            props.setString(name, ds.toString());
        }
        return props;
    }

    /**
     * Looks up and returns a named data source.
     * 
     * @return Data source corresponding to name or null if not found
     */
    public UniversalDataSource find(String name)
    {
        return manager.find(name);
    }
}