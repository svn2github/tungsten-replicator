/**
 * Tungsten: An Application Server for uni/cluster.
 * Copyright (C) 2012 Continuent Inc.
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

package com.continuent.tungsten.replicator.applier.batch;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;

import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.DatabaseFactory;

/**
 * Tests ability to handle parameterized SQL scripts using SqlScriptGenerator
 * class.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class TestJavascriptBatch
{
    private static Logger logger = Logger.getLogger(TestJavascriptBatch.class);

    private static String driver;
    private static String url;
    private static String user;
    private static String password;

    /**
     * Load test properties so we can construct DBMS connections.
     * 
     * @throws java.lang.Exception
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

        // Load driver.
        Class.forName(driver);
    }

    /**
     * Test setting up and invoking a simple script.
     */
    @Test
    public void testSimpleScript() throws Exception
    {
        String script = "function apply(csvinfo) { }";
        execute("testSimpleScript", script);
    }

    /**
     * Test calling out to the OS from within a script.
     */
    @Test
    public void testExecOSCommand() throws Exception
    {
        String script = "function apply(csvinfo) { runtime.exec('echo hello'); }";
        execute("testExecOSCommand", script);
    }

    /**
     * Verify that an exception occurs if the script has a syntax error.
     */
    @Test
    public void testSyntaxError() throws Exception
    {
        String script = "function apply(csvinfo) { this is not javascript!!! }";
        try
        {
            execute("testSyntaxError", script);
            throw new Exception("Able to execute script with syntax error: "
                    + script);
        }
        catch (ReplicatorException e)
        {
            logger.info("Caught expected exception");
        }
    }

    /**
     * Verify that a bad SQL statement throws an exception.
     */
    @Test
    public void testBadSQL() throws Exception
    {
        String script = "function apply(csvinfo) { sql.execute('bad sql'); }";
        boolean failed = false;
        try
        {
            execute("testBadSQL", script);
        }
        catch (Exception e)
        {
            logger.info("Caught expected exception");
            failed = true;
        }
        Assert.assertTrue("Query should fail", failed);
    }

    // Create context and execute script.
    private void execute(String name, String script) throws Exception
    {
        File scriptFile = writeScript(name, script);
        Database db = getDatabase();
        JavascriptExecutor exec = new JavascriptExecutor();
        exec.setConnection(db);
        exec.setScript(scriptFile.getAbsolutePath());
        exec.setShowCommands(false);
        exec.prepare(null);
        exec.execute(null);
        exec.release(null);
        db.close();
    }

    // Write script.
    public File writeScript(String name, String script) throws IOException
    {
        File testDir = new File("testJavaScriptBatch");
        testDir.mkdirs();
        File scriptFile = new File(testDir, name);
        FileWriter fw = new FileWriter(scriptFile);
        fw.write(script);
        fw.flush();
        fw.close();
        return scriptFile;
    }

    // Create database connection.
    private Database getDatabase() throws SQLException
    {
        Database db = DatabaseFactory.createDatabase(url, user, password);
        Assert.assertNotNull(db);
        db.connect();
        return db;
    }
}