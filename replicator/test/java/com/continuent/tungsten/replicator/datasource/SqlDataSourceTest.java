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
 * Contributor(s): Linas Virbalas
 */

package com.continuent.tungsten.replicator.datasource;

import java.io.File;
import java.io.FileInputStream;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.BeforeClass;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.replicator.datasource.SqlDataSource;

/**
 * Runs tests on the data source manager to ensure we can add, find, and remove
 * data sources.
 */
public class SqlDataSourceTest extends AbstractDataSourceTest
{
    private static Logger logger = Logger.getLogger(SqlDataSourceTest.class);

    // Properties used in SQL access.
    private static String driver;
    private static String url;
    private static String user;
    private static String password;
    private static String schema;
    private static String vendor;

    /**
     * Make sure we have expected test properties.
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception
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
            logger.warn("Using default values for test");

        // Set values used for test.
        driver = tp.getString("database.driver",
                "org.apache.derby.jdbc.EmbeddedDriver", true);
        url = tp.getString("database.url", "jdbc:derby:testdb;create=true",
                true);
        user = tp.getString("database.user");
        password = tp.getString("database.password");
        schema = tp.getString("database.schema", "testdb", true);
        vendor = tp.getString("database.vendor");

        // Load driver.
        Class.forName(driver);
    }

    /**
     * Set up properties used to configure the data source.
     */
    @Before
    public void setUp() throws Exception
    {
        // Create the data source properties instance. This must have bean
        // support as we will be setting bean values.
        datasourceProps = new TungstenProperties();
        datasourceProps.setBeanSupportEnabled(true);

        // Add values.
        datasourceProps.setString("serviceName", "sqlcatalog");
        datasourceProps.setLong("channels", 10);
        datasourceProps.setString("connectionSpec",
                SqlConnectionSpecGeneric.class.getName());
        datasourceProps.setString("connectionSpec.url", url);
        datasourceProps.setString("connectionSpec.user", user);
        datasourceProps.setString("connectionSpec.password", password);
        datasourceProps.setString("connectionSpec.schema", schema);
        datasourceProps.setString("connectionSpec.vendor", vendor);

        // Set the data source class.
        datasourceClass = SqlDataSource.class.getName();
    }
}