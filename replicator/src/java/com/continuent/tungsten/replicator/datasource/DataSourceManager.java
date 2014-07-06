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
 * Contributor(s): Stephane Giron
 */

package com.continuent.tungsten.replicator.datasource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.replicator.ReplicatorException;

/**
 * Manages data sources. A single manager can have any number of uniquely named
 * data sources.
 */
public class DataSourceManager
{
    private static Logger                    logger      = Logger.getLogger(DataSourceManager.class);

    // Table of currently known data sources.
    private Map<String, UniversalDataSource> datasources = new TreeMap<String, UniversalDataSource>();

    /**
     * Creates a new instance.
     */
    public DataSourceManager()
    {
    }

    /**
     * Configures and adds a new data source.
     * 
     * @param name Name of the data source
     * @param className Name of the implementing class.
     * @param attributes TungstenProperties instance containing values to assign
     *            to data source instance. If the instance uses embedded beans,
     *            the properties should have bean property support enabled
     * @return Returns instantiated and prepared data source
     * @see TungstenProperties#setBeanSupportEnabled(boolean)
     */
    public UniversalDataSource add(String name, String className,
            TungstenProperties attributes) throws ReplicatorException,
            InterruptedException
    {
        // Check for a duplicate data source, then find the class name.
        if (datasources.get(name) != null)
        {
            throw new ReplicatorException(
                    "Foiled attempt to load duplicate data source: name="
                            + name);
        }

        // Instantiate the data source class and apply properties. If successful
        // add result to the data source table.
        try
        {
            logger.info("Loading data source: name=" + name + " className="
                    + className);
            UniversalDataSource datasource = (UniversalDataSource) Class
                    .forName(className).newInstance();
            attributes.applyProperties(datasource);
            datasource.configure();
            datasource.prepare();
            datasources.put(name, datasource);
            return datasource;
        }
        catch (ReplicatorException e)
        {
            // Data source operations will throw this, so we don't need to wrap
            // it.
            throw e;
        }
        catch (Exception e)
        {
            // Any other exception is bad and must be wrapped.
            throw new ReplicatorException(
                    "Unable to instantiate and configure data source: name="
                            + name + " className=" + className + " message="
                            + e.getMessage(), e);
        }
    }

    /**
     * Returns the names of currently stored data sources.
     */
    public List<String> names()
    {
        List<String> names = new ArrayList<String>(datasources.keySet());
        return names;
    }

    /**
     * Returns the named data source or null if it does not exist.
     */
    public UniversalDataSource find(String name)
    {
        return datasources.get(name);
    }

    /**
     * Removes and deallocates a data source.
     * 
     * @param name Name of the data source to remove
     * @return Return true if the data source is found and removed
     */
    public boolean remove(String name) throws InterruptedException,
            ReplicatorException
    {
        UniversalDataSource datasource = datasources.remove(name);
        if (datasource == null)
            return false;
        else
        {
            logger.info("Releasing data source: name=" + name);
            datasource.release();
            return true;
        }
    }

    /**
     * Removes and deallocates all data sources. This should be called to ensure
     * data source resources are properly freed.
     */
    public void removeAll() throws InterruptedException, ReplicatorException
    {
        for (String name : names())
        {
            remove(name);
        }
    }
}