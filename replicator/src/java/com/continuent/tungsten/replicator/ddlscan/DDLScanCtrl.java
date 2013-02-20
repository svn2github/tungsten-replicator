/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2013 Continuent Inc.
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
 * Initial developer(s): Linas Virbalas
 * Contributor(s): 
 */

package com.continuent.tungsten.replicator.ddlscan;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.sql.SQLException;
import java.util.Properties;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.common.exec.ArgvIterator;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.conf.ReplicatorConf;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntimeConf;

/**
 * This class defines a DDLScanCtrl that implements a utility to access DDLScan
 * methods. See the printHelp() command for a description of current commands.
 * 
 * @author <a href="mailto:linas.virbalas@continuent.com">Linas Virbalas</a>
 * @version 1.0
 */
public class DDLScanCtrl
{
    /**
     * Default path to replicator.properties if user not specified other.
     */
    protected static final String defaultConfigPath = ".."
                                                            + File.separator
                                                            + "conf"
                                                            + File.separator
                                                            + "static-default.properties";

    protected static ArgvIterator argvIterator      = null;

    /**
     * Variable used in JDBC URL of Replicator's configuration file to designate
     * current schema/database.
     */
    public final static String    DBNAME_VAR        = "${DBNAME}";

    private String                url               = null;
    private String                db                = null;
    private String                user              = null;
    private String                pass              = null;

    private String                tables            = null;

    private String                templateFile      = null;
    private String                outFile           = null;

    private DDLScan               scanner           = null;

    /**
     * Creates a new <code>DDLScanCtrl</code> object from provided JDBC URL
     * connection credentials.
     * 
     * @param url JDBC URL connection string.
     * @throws Exception
     */
    public DDLScanCtrl(String url, String user, String pass, String db,
            String tables, String templateFile, String outFile)
            throws Exception
    {
        // JDBC connection string.
        this.url = url;
        this.user = user;
        this.pass = pass;

        // Tables to extract.
        this.db = db;
        this.tables = tables;

        // Output file.
        this.templateFile = templateFile;
        this.outFile = outFile;
    }

    public void prepare() throws ReplicatorException, InterruptedException
    {
        // Provide some details if we're writing to file.
        if (outFile != null)
        {
            println("url = " + url);
            println("db = " + db);
            println("user = " + user);
            println("template = " + templateFile);
        }

        try
        {
            scanner = new DDLScan(url, db, user, pass);
            scanner.prepare();
        }
        catch (SQLException e)
        {
            if (e.getMessage().contains(DBNAME_VAR))
            {
                // Known error, do not overwhelm the user - give him a tip.
                println("HINT: have you tried with \"-db dbName\" parameter?");
                fatal(e.getLocalizedMessage(), null);
            }
            else
                fatal(e.getLocalizedMessage(), e);
        }
    }

    /**
     * Scans the database for requested objects and generates output through a
     * template.
     */
    public void scanAndGenerate() throws InterruptedException,
            ReplicatorException, SQLException, IOException
    {
        Writer writer = null;

        scanner.parseTemplate(templateFile);

        if (outFile == null)
            writer = new StringWriter();
        else
            writer = new BufferedWriter(new FileWriter(new File(outFile)));

        scanner.scan(tables, writer);

        // Flush and cleanup.
        writer.flush();
        writer.close();

        if (outFile == null)
            println(writer.toString());
        else
            println("rendered to = " + outFile);
    }

    public void release()
    {
        scanner.release();
    }

    /**
     * Reads the replicator.properties.
     */
    public static TungstenProperties readConfig(String configFile)
            throws Exception
    {
        TungstenProperties conf = null;

        // Open configuration file.
        File propsFile = new File(configFile);
        if (!propsFile.exists() || !propsFile.canRead())
        {
            throw new Exception("Properties file not found: "
                    + propsFile.getAbsolutePath(), null);
        }
        conf = new TungstenProperties();

        // Read configuration.
        try
        {
            conf.load(new FileInputStream(propsFile));
        }
        catch (IOException e)
        {
            throw new Exception(
                    "Unable to read properties file: "
                            + propsFile.getAbsolutePath() + " ("
                            + e.getMessage() + ")", null);
        }
        return conf;
    }

    /**
     * Main method to run utility.
     * 
     * @param argv optional command string
     */
    public static void main(String argv[])
    {
        try
        {
            // Command line parameters and options.
            String configFile = null;
            String service = null;
            String command = null;
            String templateFile = null;
            String user = null;
            String pass = null;
            String url = null;
            String tables = null;
            String db = null;
            String outFile = null;

            // Parse command line arguments.
            ArgvIterator argvIterator = new ArgvIterator(argv);
            String curArg = null;
            while (argvIterator.hasNext())
            {
                curArg = argvIterator.next();
                if ("-conf".equals(curArg))
                {
                    if (argvIterator.hasNext())
                        configFile = argvIterator.next();
                }
                else if ("-service".equals(curArg))
                {
                    if (argvIterator.hasNext())
                        service = argvIterator.next();
                }
                else if ("-db".equals(curArg))
                {
                    if (argvIterator.hasNext())
                        db = argvIterator.next();
                }
                else if ("-user".equals(curArg))
                {
                    if (argvIterator.hasNext())
                        user = argvIterator.next();
                }
                else if ("-pass".equals(curArg))
                {
                    if (argvIterator.hasNext())
                        pass = argvIterator.next();
                }
                else if ("-url".equals(curArg))
                {
                    if (argvIterator.hasNext())
                        url = argvIterator.next();
                }
                else if ("-tables".equals(curArg))
                {
                    if (argvIterator.hasNext())
                        tables = argvIterator.next();
                }
                else if ("-template".equals(curArg))
                {
                    if (argvIterator.hasNext())
                        templateFile = argvIterator.next();
                }
                else if ("-out".equals(curArg))
                {
                    if (argvIterator.hasNext())
                        outFile = argvIterator.next();
                }
                else if (curArg.startsWith("-"))
                    fatal("Unrecognized option: " + curArg, null);
                else
                    command = curArg;
            }

            if (command != null && "help".equals(command))
            {
                printHelp();
                succeed();
            }
            else if (templateFile == null)
            {
                println("Template file is not provided! Use -template parameter.");
                printHelp();
                fail();
            }
            else if (((configFile != null || service != null) && (user != null
                    && pass != null && url != null))
                    || (configFile != null && service != null))
            {
                fatal("Use -user, -pass and -url or -conf or -service options. Don't mix.",
                        null);
            }

            if (db == null)
                fatal("Database is not provided! Use -db parameter.", null);

            // Construct actual DDLScanCtrl and call methods based on a
            // parsed user input.
            if (user == null || pass == null || url == null)
            {
                // Credentials not provided, retrieve from configuration.
                if (configFile == null)
                {
                    if (service == null)
                    {
                        // Nothing is given. Retrieve configuration file of
                        // default service.
                        configFile = lookForConfigFile();
                        if (configFile == null)
                        {
                            fatal("You must specify either a config file or a service name (-conf or -service)",
                                    null);
                        }
                    }
                    else
                    {
                        // Retrieve configuration file of a given service.
                        ReplicatorRuntimeConf runtimeConf = ReplicatorRuntimeConf
                                .getConfiguration(service);
                        configFile = runtimeConf.getReplicatorProperties()
                                .getAbsolutePath();
                    }
                }

                // Read connection info from configuration file.
                TungstenProperties properties = readConfig(configFile);
                if (properties != null)
                {
                    // Get JDBC URL and substitute the database name to avoid
                    // errors.
                    Properties jProps = new Properties();
                    jProps.setProperty("URL", properties
                            .getString(ReplicatorConf.RESOURCE_JDBC_URL));
                    if (db != null)
                        jProps.setProperty("DBNAME", db);
                    TungstenProperties.substituteSystemValues(jProps);
                    url = jProps.getProperty("URL");

                    // Get login.
                    user = properties.getString(ReplicatorConf.GLOBAL_DB_USER);
                    pass = properties
                            .getString(ReplicatorConf.GLOBAL_DB_PASSWORD);

                    if (user == null || url == null)
                        throw new ReplicatorException(
                                "Configuration file doesn't have JDBC URL credentials");
                }
                else
                    throw new ReplicatorException(
                            "Unable to read configuration file " + configFile);
            }

            // Construct DDLScanCtrl from JDBC URL credentials.
            DDLScanCtrl ddlScanManager = new DDLScanCtrl(url, user, pass, db,
                    tables, templateFile, outFile);

            if (tables == null && outFile != null)
                println("Tables not specified - extracting everything!");

            ddlScanManager.prepare();

            ddlScanManager.scanAndGenerate();

            ddlScanManager.release();
        }
        catch (Throwable t)
        {
            fatal("Fatal error: " + t.getMessage(), t);
        }
    }

    /**
     * Return the service configuration file if there is one and only one file
     * that matches the static-svcname.properties pattern.
     */
    private static String lookForConfigFile()
    {
        File configDir = ReplicatorRuntimeConf.locateReplicatorConfDir();
        FilenameFilter propFileFilter = new FilenameFilter()
        {
            public boolean accept(File fdir, String fname)
            {
                if (fname.startsWith("static-")
                        && fname.endsWith(".properties"))
                    return true;
                else
                    return false;
            }
        };
        File[] propertyFiles = configDir.listFiles(propFileFilter);
        if (propertyFiles.length == 1)
            return propertyFiles[0].getAbsolutePath();
        else
            return null;
    }

    protected static void printHelp()
    {
        println("DDLScan Utility");
        println("Syntax: ddlscan [connection|conf-options] [scan-spec] -db <db> -template <file> [out-options]");
        println("Conf options:");
        println("  -conf path     - Path to a static-<svc>.properties file to read JDBC");
        println("                   connection address and credentials OR");
        println("  -service name  - Name of a replication service instead of path to config");
        println("OR connection options:");
        println("  -user user     - JDBC username");
        println("  -pass secret   - JDBC password");
        println("  -url jdbcUrl   - JDBC connection string (use single quotes to escape)");
        println("Schema scan specification:");
        println("  -tables regex  - Regular expression enabled list defining tables to find");
        println("Global options:");
        println("  -db db         - Database to use (will substitute "
                + DBNAME_VAR + " in the URL, if needed)");
        println("  -template file - Specify template file to render");
        println("  -out file      - Render to file (print to stdout if not specified)");
        println("  help           - Print this help display");
    }

    /**
     * Appends a message to a given stringBuilder, adds a newline character at
     * the end.
     * 
     * @param msg String to print.
     * @param stringBuilder StringBuilder object to add a message to.
     */
    private static void println(StringBuilder stringBuilder, String msg)
    {
        stringBuilder.append(msg);
        stringBuilder.append("\n");
    }

    /**
     * Print a message to stdout with trailing new line character.
     * 
     * @param msg
     */
    protected static void println(String msg)
    {
        System.out.println(msg);
    }

    /**
     * Print a message to stdout without trailing new line character.
     * 
     * @param msg
     */
    protected static void print(String msg)
    {
        System.out.print(msg);
    }

    /**
     * Abort following a fatal error.
     * 
     * @param msg
     * @param t
     */
    protected static void fatal(String msg, Throwable t)
    {
        System.out.println(msg);
        if (t != null)
            t.printStackTrace();
        fail();
    }

    /**
     * Exit with a process failure code.
     */
    protected static void fail()
    {
        System.exit(1);
    }

    /**
     * Exit with a process success code.
     */
    protected static void succeed()
    {
        System.exit(0);
    }

    /**
     * Reads a character from stdin, blocks until it is not received.
     * 
     * @return true if use pressed `y`, false otherwise.
     */
    protected static boolean readYes() throws IOException
    {
        return (System.in.read() == 'y');
    }

    /**
     * Returns a value of a given Boolean object or false if the object is null.
     * 
     * @param bool Boolean object to check and return.
     * @return the value of a given Boolean object or false if the object is
     *         null.
     */
    protected static boolean getBoolOrFalse(Boolean bool)
    {
        if (bool != null)
            return bool;
        else
            return false;
    }
}
