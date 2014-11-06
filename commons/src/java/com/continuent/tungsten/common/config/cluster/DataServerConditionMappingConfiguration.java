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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.cluster.resource.ResourceState;
import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.common.mysql.MySQLIOs.ExecuteQueryStatus;
import com.continuent.tungsten.common.utils.CLLogLevel;
import com.continuent.tungsten.common.utils.CLUtils;

/**
 * Loads a default condition mapping configuration and an optional 'overrides'
 * condition mapping configuration and serves as the main interface to the
 * properties exported by these configurations.
 * 
 * @author <a href="mailto:edward.archibald@continuent.com">Edward Archibald</a>
 * @version 1.0
 */
public class DataServerConditionMappingConfiguration
        extends ClusterConfiguration
{

    private static Logger                                              logger                         = Logger.getLogger(DataServerConditionMapping.class);

    private static Map<ExecuteQueryStatus, DataServerConditionMapping> overrides                      = null;
    private static Map<ExecuteQueryStatus, DataServerConditionMapping> defaults                       = null;
    private static DataServerConditionMappingConfiguration             instance                       = null;

    private static long                                                defaultStateMappingModified    = -1;
    private static long                                                overriddenStateMappingModified = -1;

    private DataServerConditionMappingConfiguration()
            throws ConfigurationException
    {
        super(null);

        loadConditionMappingIfNeeded();
    }

    private static Map<ExecuteQueryStatus, DataServerConditionMapping> getMappings(
            TungstenProperties mappingProps)
    {
        Map<ExecuteQueryStatus, DataServerConditionMapping> mapping = new HashMap<ExecuteQueryStatus, DataServerConditionMapping>();
        for (ExecuteQueryStatus status : ExecuteQueryStatus.values())
        {
            TungstenProperties mappingForStatus = mappingProps.subset(status
                    .toString().toLowerCase() + ".", true);

            if (mappingForStatus != null)
            {
                if (mappingForStatus.size() != 3)
                {
                    logger.warn(String
                            .format("The mapping for status '%s' is malformed: only contains %d elements",
                                    status.toString().toLowerCase(),
                                    mappingForStatus.size()));
                    continue;
                }
                mapping.put(status, new DataServerConditionMapping(status,
                        mappingForStatus));
            }
        }

        return mapping;
    }

    public static DataServerConditionMappingConfiguration getInstance()
            throws ConfigurationException
    {
        if (instance == null)
        {
            instance = new DataServerConditionMappingConfiguration();
        }
        else
        {
            loadConditionMappingIfNeeded();
        }

        return instance;
    }

    public void store() throws ConfigurationException
    {
        TungstenProperties propsToStore = new TungstenProperties();
        for (ExecuteQueryStatus key : overrides.keySet())
        {
            String value = overrides.get(key).toString().toLowerCase();
            propsToStore.setString(key.toString().toLowerCase(), value);

        }

        store(ConfigurationConstants.CLUSTER_STATE_MAP_OVERRIDE_PROPS,
                propsToStore);
    }

    /**
     * Returns the full path of the data services configuration file.
     */
    public String getConfigFileNameInUse()
    {
        return System.getProperty(ConfigurationConstants.CLUSTER_HOME)
                + File.separator
                + ConfigurationConstants.CLUSTER_STATE_MAP_OVERRIDE_PROPS;
    }

    /**
     * Returns the ResourceState currently mapped to the query status.
     * 
     * @param queryStatus
     * @throws ConfigurationException
     */
    public static DataServerConditionMapping getConditionMapping(
            ExecuteQueryStatus queryStatus)
    {
        try
        {
            getInstance();
        }
        catch (ConfigurationException c)
        {
            logger.error(String.format(
                    "Exception while attempting to get condition mapping: %s",
                    c));

            return new DataServerConditionMapping();
        }

        DataServerConditionMapping mapping = overrides.get(queryStatus);

        if (mapping == null)
        {
            mapping = defaults.get(queryStatus);

            if (mapping == null)
            {

                mapping = new DataServerConditionMapping(queryStatus);

                logger.warn(String
                        .format("No default mapping found for condition '%s'. Returning: %s",
                                queryStatus, mapping));
            }
        }

        return mapping;
    }

    public static String showStateMapping()
    {
        StringBuilder builder = new StringBuilder();

        if (defaults == null)
        {
            return builder.append("STATEMAP IS EMPTY").toString();
        }

        for (ExecuteQueryStatus key : ExecuteQueryStatus.values())
        {
            DataServerConditionMapping mapping = overrides.get(key);

            if (mapping != null)
            {
                builder.append(String.format("%s : OVERRIDDEN\n", mapping));
            }
            else
            {
                mapping = defaults.get(key);

                if (mapping == null)
                {
                    mapping = new DataServerConditionMapping(key);

                    logger.warn(String
                            .format("No default mapping found for condition '%s'. Using: %s",
                                    key, mapping));
                }
                builder.append(String.format("%s\n", mapping));
            }
        }

        return builder.toString();
    }

    public static Map<ExecuteQueryStatus, DataServerConditionMapping> getOverrides()
    {
        return overrides;
    }

    public static Map<ExecuteQueryStatus, DataServerConditionMapping> getDefaults()
    {
        return defaults;
    }

    /**
     * Indicates whether a given config file was modified since the last time it
     * was accessed based on the time modified that is passed in.
     * 
     * @param configFileName
     * @param timeLastModified
     * @return
     */
    private static long configFileLastModified(String configFileName,
            long timeLastModified)
    {

        File configFile = new File(getGlobalConfigDirName(clusterHomeName),
                configFileName);

        if (!configFile.exists())
        {
            return timeLastModified;
        }

        if (configFile.lastModified() > timeLastModified)
        {
            return configFile.lastModified();
        }

        return timeLastModified;
    }

    /**
     * Loads the condition mapping, initially, and any time one of the condition
     * mapping files is modified.
     * 
     * @throws ConfigurationException
     */
    private static synchronized void loadConditionMappingIfNeeded()
            throws ConfigurationException
    {
        boolean loadNeeded = false;

        long lastModified = -1;

        if ((lastModified = configFileLastModified(
                ConfigurationConstants.CLUSTER_STATE_MAP_DEFAULT_PROPS,
                defaultStateMappingModified)) > defaultStateMappingModified)
        {
            CLUtils.println(String.format("%s CONDITION MAPPING FROM %s/%s",
                    (defaultStateMappingModified == -1
                            ? "LOADING"
                            : "RELOADING"),
                    getGlobalConfigDirName(clusterHomeName),
                    ConfigurationConstants.CLUSTER_STATE_MAP_DEFAULT_PROPS),
                    CLLogLevel.detailed);
            defaultStateMappingModified = lastModified;
            loadNeeded = true;
        }

        if ((lastModified = configFileLastModified(
                ConfigurationConstants.CLUSTER_STATE_MAP_OVERRIDE_PROPS,
                overriddenStateMappingModified)) > overriddenStateMappingModified)
        {
            CLUtils.println(String.format("%s CONDITION MAPPING FROM %s/%s",
                    (overriddenStateMappingModified == -1
                            ? "LOADING"
                            : "RELOADING"),
                    getGlobalConfigDirName(clusterHomeName),
                    ConfigurationConstants.CLUSTER_STATE_MAP_OVERRIDE_PROPS),
                    CLLogLevel.detailed);
            overriddenStateMappingModified = lastModified;
            loadNeeded = true;
        }

        if (!loadNeeded)
        {
            return;
        }

        TungstenProperties mapping = getConfiguration(ConfigurationConstants.CLUSTER_STATE_MAP_DEFAULT_PROPS);
        defaults = getMappings(mapping);

        try
        {
            mapping = getConfiguration(ConfigurationConstants.CLUSTER_STATE_MAP_OVERRIDE_PROPS);
            overrides = getMappings(mapping);
        }
        catch (ConfigurationException c)
        {
            overrides = new HashMap<ExecuteQueryStatus, DataServerConditionMapping>();
        }

        CLUtils.println(showStateMapping(), CLLogLevel.detailed);

    }
}
