/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2011 Continuent Inc.
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
 * Initial developer(s): Gilles Rayrat
 * Contributor(s):
 */

package com.continuent.tungsten.common.config.cluster;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import com.continuent.tungsten.common.cluster.resource.ResourceState;
import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.common.mysql.MySQLIOs.ExecuteQueryStatus;

/**
 * Loads a mapping of query execution statuses to ResourceState. Used primarily
 * by manager code that determines whether a given status maps to a stopped or
 * other state.
 * 
 * @author <a href="mailto:edward.archibald@continuent.com">Edward Archibald</a>
 * @version 1.0
 */
public class QueryStatusToResourceStateConfiguration
        extends ClusterConfiguration
{
    /** Composite Data Source name <> list of managers */
    private static Map<ExecuteQueryStatus, ResourceState>  stateMap = new HashMap<ExecuteQueryStatus, ResourceState>();
    private static QueryStatusToResourceStateConfiguration instance = null;

    private QueryStatusToResourceStateConfiguration()
            throws ConfigurationException
    {
        // no cluster name here
        super(null);
        load(ConfigurationConstants.CLUSTER_STATE_MAP_PROPS);

        for (String key : props.keyNames())
        {
            try
            {
                ExecuteQueryStatus queryStatus = ExecuteQueryStatus.valueOf(key
                        .toUpperCase().trim());

                ResourceState mappedState = ResourceState.valueOf(props
                        .getString(key.toLowerCase().trim()).toUpperCase()
                        .trim());

                stateMap.put(queryStatus, mappedState);
            }
            catch (Exception e)
            {
                throw new ConfigurationException(String.format(
                        "Exception while loading status map: %s", e.toString()));
            }
        }
    }

    public static QueryStatusToResourceStateConfiguration getInstance()
            throws ConfigurationException
    {
        if (instance == null)
        {
            instance = new QueryStatusToResourceStateConfiguration();
        }

        return instance;
    }

    public void store() throws ConfigurationException
    {
        TungstenProperties propsToStore = new TungstenProperties();
        for (ExecuteQueryStatus key : stateMap.keySet())
        {
            String value = stateMap.get(key).toString().toLowerCase();
            propsToStore.setString(key.toString().toLowerCase(), value);

        }

        store(ConfigurationConstants.CLUSTER_STATE_MAP_PROPS, propsToStore);
    }

    /**
     * Returns the full path of the data services configuration file.
     * 
     * @return
     */
    public String getConfigFileNameInUse()
    {
        return System.getProperty(ConfigurationConstants.CLUSTER_HOME)
                + File.separator
                + ConfigurationConstants.CLUSTER_STATE_MAP_PROPS;
    }

    /**
     * Returns the ResourceState currently mapped to the query status.
     * 
     * @param queryStatus
     * @return
     * @throws ConfigurationException
     */
    public static ResourceState getMappedState(ExecuteQueryStatus queryStatus)
            throws ConfigurationException
    {
        getInstance();
        ResourceState mappedState = stateMap.get(queryStatus);

        if (mappedState == null)
        {
            throw new ConfigurationException(String.format(
                    "No mapping found for query status=%s", queryStatus));
        }

        return mappedState;

    }

    public static String showStateMapping()
    {
        StringBuilder builder = new StringBuilder();

        for (ExecuteQueryStatus key : stateMap.keySet())
        {
            builder.append(String.format("%s=%s\n", key, stateMap.get(key)));
        }

        return builder.toString();
    }
}
