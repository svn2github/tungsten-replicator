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

import junit.framework.Assert;

import org.junit.BeforeClass;
import org.junit.Test;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.common.csv.CsvSpecification;

/**
 * Runs tests on the data source manager to ensure we can add, find, and remove
 * data sources.
 */
public class TestDataSourceManager
{
    /**
     * Make sure we have expected test properties.
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
    }

    /**
     * Verify that if you add a data source it is possible to fetch the data
     * source back and get the same properties as originally added and also to
     * remove the data source.
     */
    @Test
    public void testAddRemoveDatasource() throws Exception
    {
        // Create the data source definition.
        TungstenProperties props = new TungstenProperties();
        props.setString("datasources.test", SampleDataSource.class.getName());
        props.setString("serviceName", "mytest");
        props.setLong("channels", 3);
        props.setString("myParameter", "some value");

        // Ensure that data source does not already exist.
        DataSourceManager cm = new DataSourceManager();
        cm.remove("test");
        Assert.assertNull("Ensuring data source does not exist prior to test",
                cm.find("test"));

        // Add new data source, then fetch it back and confirm field values.
        cm.add("test", SampleDataSource.class.getName(), props);
        SampleDataSource c = (SampleDataSource) cm.find("test");
        Assert.assertNotNull("Data source should be available", c);
        Assert.assertEquals("Comparing channels", 3, c.getChannels());
        Assert.assertEquals("Comparing service name", "mytest",
                c.getServiceName());

        // Remove the data source and confirm that it succeeds.
        Assert.assertEquals("Testing data source removal", c,
                cm.remove("test"));

        // Confirm that attempts to remove or get the data source now fail.
        Assert.assertNull("Ensuring data source does not exist after removal",
                cm.find("test"));
        Assert.assertNull("Ensuring data source cannot be removed twice",
                cm.remove("test"));
    }

    /**
     * Verify that we can add two data sources without errors and then remove
     * them one by one.
     */
    @Test
    public void testAddTwoDataSources() throws Exception
    {
        // Create the data source definitions using a single properties file.
        TungstenProperties props1 = new TungstenProperties();
        props1.setString("serviceName", "mytest1");
        TungstenProperties props2 = new TungstenProperties();
        props2.setString("serviceName", "mytest2");

        // Ensure that data sources do not already exist.
        DataSourceManager cm = new DataSourceManager();
        Assert.assertNull("Ensuring data source does not exist prior to test",
                cm.find("test1"));
        Assert.assertNull("Ensuring data source does not exist prior to test",
                cm.find("test2"));

        // Add data sources and confirm that both names are present and that the
        // count of names is 2.
        cm.addAndPrepare("test1", SampleDataSource.class.getName(), props1);
        cm.addAndPrepare("test2", SampleDataSource.class.getName(), props2);
        Assert.assertEquals("Checking number of names", 2, cm.names().size());

        SampleDataSource c1 = (SampleDataSource) cm.find("test1");
        Assert.assertNotNull("Data source should be available", c1);
        Assert.assertEquals("Data source name set", "test1", c1.getName());
        Assert.assertEquals("Comparing service name", "mytest1",
                c1.getServiceName());

        SampleDataSource c2 = (SampleDataSource) cm.find("test2");
        Assert.assertNotNull("Data source should be available", c2);
        Assert.assertEquals("Comparing service name", "mytest2",
                c2.getServiceName());

        // Remove one data source and confirm that it succeeds.
        Assert.assertEquals("Testing data source removal", c1,
                cm.remove("test1"));
        Assert.assertEquals("Checking number of names", 1, cm.names().size());
        Assert.assertNull("Data source not should be available",
                cm.find("test1"));
        Assert.assertNotNull("Data source should be available",
                cm.find("test2"));

        // Confirm that removeAll removes the remaining data source.
        cm.removeAndReleaseAll();
        Assert.assertEquals("Checking number of names", 0, cm.names().size());
        Assert.assertNull("Data source should not be available",
                cm.find("test2"));
    }

    /**
     * Verify that if you add a data source with a CsvSpecification you can then
     * get back that specification and generate properly configured CsvWriter
     * and CsvReader instances.
     */
    @Test
    public void testDataSourceWithCsvSpec() throws Exception
    {
        // Create the data source definition.
        TungstenProperties props = new TungstenProperties();
        props.setString("serviceName", "mytest");
        props.setString("csv", CsvSpecification.class.getName());
        props.setString("csv.fieldSeparator", ":");
        props.setString("csv.recordSeparator", "\u0002");
        props.setBoolean("csv.useQuotes", true);
        props.setBeanSupportEnabled(true);

        // Ensure that data source does not already exist.
        DataSourceManager cm = new DataSourceManager();
        cm.remove("test");
        Assert.assertNull("Ensuring data source does not exist prior to test",
                cm.find("test"));

        // Add new data source, then fetch it back.
        cm.addAndPrepare("test", SampleDataSource.class.getName(), props);
        SampleDataSource c = (SampleDataSource) cm.find("test");
        Assert.assertNotNull("Data source should be available", c);

        // Confirm existence of CsvSpecification and validate properties.
        CsvSpecification csv = c.getCsv();
        Assert.assertNotNull("CsvSpecification should be available", csv);
        Assert.assertEquals("Checking field separator", ":",
                csv.getFieldSeparator());
        Assert.assertEquals("Checking record separator", "\u0002",
                csv.getRecordSeparator());
        Assert.assertEquals("Checking use quotes", true, csv.isUseQuotes());

        // Clean up data source.
        cm.removeAndRelease("test");
    }

}