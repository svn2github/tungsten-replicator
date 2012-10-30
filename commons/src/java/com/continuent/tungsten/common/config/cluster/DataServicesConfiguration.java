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
import java.util.List;
import java.util.Map;

import com.continuent.tungsten.common.config.TungstenProperties;

/**
 * Provides the location as an IP addresses or host names of managers in charge
 * of each data services available in the cluster
 * 
 * @author <a href="mailto:gilles.rayrat@continuent.com">Gilles Rayrat</a>
 * @version 1.0
 */
public class DataServicesConfiguration extends ClusterConfiguration
{
    /** Composite Data Source name <> list of managers */
    private static Map<String, List<String>> physicalDataServiceManagers = new HashMap<String, List<String>>();
    private static DataServicesConfiguration instance                    = null;

    private DataServicesConfiguration() throws ConfigurationException
    {
        // no cluster name here
        super(null);
        load(ConfigurationConstants.TR_SERVICES_PROPS);
        for (String cds : props.keyNames())
        {
            physicalDataServiceManagers.put(cds, props.getStringList(cds));
        }
    }

    public static DataServicesConfiguration getInstance()
            throws ConfigurationException
    {
        if (instance == null)
        {
            instance = new DataServicesConfiguration();
        }

        return instance;
    }

    public void addDataService(String dataServiceName, List<String> managerList)
            throws ConfigurationException
    {
        if (physicalDataServiceManagers.get(dataServiceName) != null)
        {
            throw new ConfigurationException(String.format(
                    "Data service '%s' already exists.", dataServiceName));
        }

        physicalDataServiceManagers.put(dataServiceName, managerList);
    }

    public void store() throws ConfigurationException
    {
        TungstenProperties propsToStore = new TungstenProperties();
        for (String key : physicalDataServiceManagers.keySet())
        {
            String value = TungstenProperties
                    .listToString(physicalDataServiceManagers.get(key));
            propsToStore.setString(key, value);

        }

        store(ConfigurationConstants.TR_SERVICES_PROPS, propsToStore);
    }

    static public Map<String, List<String>> getPhyicalDataServiceManagersList()
    {
        return physicalDataServiceManagers;
    }

    /**
     * If the given data source name appears in our list of composite data
     * sources, it means that it is a composite data source
     * 
     * @param ds name of the data source for which to determine origin
     * @return true if the given name is a composite data source
     */
    public static boolean isPhysicalDataService(String ds)
    {
        return physicalDataServiceManagers.containsKey(ds);
    }

    /**
     * Returns the full path of the data services configuration file.
     * 
     * @return
     */
    public String getConfigFileNameInUse()
    {
        return System.getProperty(ConfigurationConstants.CLUSTER_HOME)
                + File.separator + ConfigurationConstants.TR_SERVICES_PROPS;
    }
}
