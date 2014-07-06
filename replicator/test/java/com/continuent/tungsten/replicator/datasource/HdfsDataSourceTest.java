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

import java.io.File;
import java.io.FileInputStream;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.common.config.TungstenPropertiesIO;

/**
 * Implements a test on file data source operations.
 */
public class HdfsDataSourceTest extends AbstractDataSourceTest
{
    private static Logger logger = Logger.getLogger(HdfsDataSourceTest.class);

    private File          hdfsPropFile;

    /**
     * Set up properties used to configure the data source.
     */
    @Before
    public void setUp() throws Exception
    {
        // Set test.properties file name.
        String testPropertiesName = System.getProperty("test.properties");
        if (testPropertiesName == null)
        {
            testPropertiesName = "test.properties";
            logger.info("Setting test.properties file name to default: test.properties");
        }

        // Load test properties file.
        TungstenProperties tp = new TungstenProperties();
        File f = new File(testPropertiesName);
        if (f.canRead())
        {
            logger.info("Reading test.properties file: " + testPropertiesName);
            FileInputStream fis = new FileInputStream(f);
            tp.load(fis);
            fis.close();
        }
        else
            logger.warn("Could not find test.properties file!");

        // Find values used for test. If we don't have a URI for HDFS
        // access, we don't generate any properties so test will not run.
        String hdfsUri = tp.getString("hdfs.uri");
        if (hdfsUri != null)
        {
            // Create the data source definition.
            datasourceProps = new TungstenProperties();
            datasourceProps.setString("serviceName", "hdfscatalog");
            datasourceProps.setLong("channels", 10);
            datasourceProps.set("hdfsUri", tp.getString("hdfs.uri"));
            datasourceProps.set("directory", tp.getString("hdfs.directory"));

            // Create an hdfs config properties file.
            TungstenProperties hdfsProps = tp.subset("hdfs.config.", true);
            hdfsPropFile = File.createTempFile("hdfs", ".properties");
            TungstenPropertiesIO propsIO = new TungstenPropertiesIO(
                    hdfsPropFile);
            propsIO.setFormat(TungstenPropertiesIO.JAVA_PROPERTIES);
            propsIO.write(hdfsProps, true);

            datasourceProps.set("hdfsConfigProperties",
                    hdfsPropFile.getAbsolutePath());

            // Set the data source class.
            datasourceClass = HdfsDataSource.class.getName();
        }
    }

    /**
     * Remove the properties file.
     */
    @After
    public void teardown()
    {
        if (hdfsPropFile != null)
        {
            hdfsPropFile.delete();
        }
    }
}