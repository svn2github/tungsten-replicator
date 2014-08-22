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
     * Returns the number of data sources currently under management.
     */
    public int count()
    {
        return datasources.size();
    }

    /**
     * Adds a new data source. Clients <em>must</em> configure and prepare the
     * data source through the corresponding methods to make use of it.
     * 
     * @param name Name of the data source
     * @param className Name of the implementing class.
     * @param attributes TungstenProperties instance containing values to assign
     *            to data source instance. If the instance uses embedded beans,
     *            the properties should have bean property support enabled
     * @return Returns instantiated data source
     * @see TungstenProperties#setBeanSupportEnabled(boolean)
     * @see #addAndPrepare(String, String, TungstenProperties)
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
            datasource.setName(name);
            datasources.put(name, datasource);
            return datasource;
        }
        catch (Exception e)
        {
            throw new ReplicatorException(
                    "Unable to instantiate data source: name=" + name
                            + " className=" + className + " message="
                            + e.getMessage(), e);
        }
    }

    /**
     * Configures a data source.
     * 
     * @param name Name of the data source.
     */
    public void configure(String name) throws ReplicatorException,
            InterruptedException
    {
        UniversalDataSource ds = find(name);
        ds.configure();
    }

    /**
     * Prepares a data source.
     * 
     * @param name Name of the data source.
     */
    public void prepare(String name) throws ReplicatorException,
            InterruptedException
    {
        UniversalDataSource ds = find(name);
        ds.prepare();
    }

    /**
     * Adds configures, and prepares a data source in a single step.
     * 
     * @param name Name of the data source
     * @param className Name of the implementing class.
     * @param attributes TungstenProperties instance containing values to assign
     *            to data source instance. If the instance uses embedded beans,
     *            the properties should have bean property support enabled
     * @return Returns instantiated data source
     */
    public UniversalDataSource addAndPrepare(String name, String className,
            TungstenProperties attributes) throws InterruptedException,
            ReplicatorException
    {
        UniversalDataSource ds = this.add(name, className, attributes);
        ds.configure();
        ds.prepare();
        return ds;
    }

    /**
     * Releases a data source.
     * 
     * @param name Name of the data source.
     */
    public void release(String name) throws ReplicatorException,
            InterruptedException
    {
        UniversalDataSource ds = find(name);
        ds.release();
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
     * Removes a data source.
     * 
     * @param name Name of the data source to remove
     * @return Return data source or null if not found
     */
    public UniversalDataSource remove(String name) throws InterruptedException
    {
        return datasources.remove(name);
    }

    /**
     * Removes and deallocates a data source.
     * 
     * @param name Name of the data source to remove
     * @return Return data source or null if not found
     */
    public UniversalDataSource removeAndRelease(String name)
            throws InterruptedException, ReplicatorException
    {
        UniversalDataSource ds = remove(name);
        if (name != null)
            ds.release();
        return ds;
    }

    /**
     * Removes and deallocates all data sources. This should be called to ensure
     * data source resources are properly freed.
     */
    public void removeAndReleaseAll() throws InterruptedException
    {
        for (String name : names())
        {
            UniversalDataSource ds = remove(name);
            try
            {
                ds.release();
            }
            catch (ReplicatorException e)
            {
                logger.warn("Unable to release data source: name=" + name, e);
            }
        }
    }
}