/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2013 Continuent Inc.
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

package com.continuent.tungsten.common.file;

import java.io.File;
import java.io.FileInputStream;
import java.net.URI;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;

import com.continuent.tungsten.common.config.TungstenProperties;

/**
 * Implements unit test for HDFS implementation of FileIO. This requires data
 * from the test.properties file to locate the HDFS cluster used for testing.
 */
public class HdfsFileIOTest extends AbstractFileIOTest
{
    private static Logger logger = Logger.getLogger(Logger.class);

    // URI to connect to HDFS; if unavailable we cannot connect.
    private String        hdfsUri;

    /**
     * Find the HDFS URI and login from that location.
     * 
     * @throws java.lang.Exception
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

        // Load properties file.
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

        // Find values used for test.
        hdfsUri = tp.getString("hdfs.uri");
        TungstenProperties hdfsProps = tp.subset("hdfs.config.", true);

        // Define our FileIO instance for Hadoop.
        if (hdfsUri == null)
            logger.info("HDFS URI required for this test is not set");
        else
        {
            URI uri = new URI(hdfsUri);
            this.fileIO = new HdfsFileIO(uri, hdfsProps);
        }
    }

    /**
     * Teardown.
     * 
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
    }
}