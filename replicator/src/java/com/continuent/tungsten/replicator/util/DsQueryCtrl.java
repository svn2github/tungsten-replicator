/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2014 Continuent Inc.
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
 * Initial developer(s): Stephane Giron
 * Contributor(s): 
 */

package com.continuent.tungsten.replicator.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.common.exec.ArgvIterator;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.DatabaseFactory;

/**
 * This class defines a THLManagerCtrl that implements a utility to access
 * THLManager methods. See the printHelp() command for a description of current
 * commands.
 * 
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class DsQueryCtrl
{
    protected static ArgvIterator argvIterator = null;

    /**
     * Connect to the underlying database containing THL.
     * 
     * @throws ReplicatorException
     */
    public void prepare() throws ReplicatorException, InterruptedException
    {
    }

    /**
     * Disconnect from the THL database.
     */
    public void release()
    {
    }

    /**
     * Main method to run utility.
     * 
     * @param argv optional command string
     */
    @SuppressWarnings("unchecked")
    public static void main(String argv[])
    {
        try
        {
            // Command line parameters and options.
            String configFile = null;
            String fileName = null;
            String user = null, password = null, url = null;

            // Parse command line arguments.
            ArgvIterator argvIterator = new ArgvIterator(argv);
            if (!argvIterator.hasNext())
            {
                printHelp();
                System.exit(0);
            }

            String curArg = null;
            while (argvIterator.hasNext())
            {
                curArg = argvIterator.next();
                if ("-conf".equals(curArg))
                {
                    configFile = argvIterator.next();
                }
                else if ("-file".equals(curArg))
                {
                    fileName = argvIterator.next();
                }
                else if ("-user".equals(curArg))
                {
                    user = argvIterator.next();
                }
                else if ("-password".equals(curArg))
                {
                    if (System.console() == null)
                        fatal("Console not available. Unable to type password interactively.",
                                null);
                    System.out.print("Enter password: ");
                    password = new String(System.console().readPassword());
                }
                else if ("-url".equals(curArg))
                {
                    url = argvIterator.next();
                }

                else if (curArg.startsWith("-"))
                    fatal("Unrecognized option: " + curArg, null);
            }

            if (configFile != null)
            {
                File file = new File(configFile);
                if (!file.exists() || !file.canRead())
                    fatal("Unable to read config file (" + configFile + ")",
                            null);
                TungstenProperties props = new TungstenProperties();
                props.load(new FileInputStream(file));

                if (user == null)
                    user = props.getString("user", "", true);

                if (password == null)
                    password = props.getString("password", "", true);

                if (url == null)
                    url = props.getString("url", "", true);
            }

            if (url == null)
                fatal("URL must be provided (either using -url option or in configuration file)",
                        null);
            BufferedReader br = null;

            boolean readingFromStdIn = false;
            if (fileName == null)
            {
                readingFromStdIn = true;
                br = new BufferedReader(new InputStreamReader(System.in));
            }
            else
            {
                File file = new File(fileName);
                if (!file.exists() || !file.canRead())
                    fatal("Unable to read sql file (" + fileName + ")", null);

                br = new BufferedReader(new FileReader(file));
            }

            Database database = DatabaseFactory.createDatabase(url, user,
                    password);

            database.connect();

            String sql = null;

            SQLException sqlEx;

            JSONArray jsonArr = new JSONArray();

            while ((sql = br.readLine()) != null)
            {
                sqlEx = null;
                sql = sql.trim();
                if (readingFromStdIn && sql.length() == 0)
                    break;
                else if (sql.startsWith("#") || sql.length() == 0)
                    continue;

                LinkedHashMap<String, Object> jsonObj = new LinkedHashMap<String, Object>();
                jsonArr.add(jsonObj);
                Statement stmt = null;
                int rc = 0;
                try
                {
                    stmt = database.createStatement();

                    boolean isRS = false;
                    jsonObj.put("statement", sql);

                    try
                    {
                        isRS = stmt.execute(sql);
                        rc = 0;
                    }
                    catch (SQLException e)
                    {
                        rc = e.getErrorCode();
                        sqlEx = e;
                    }
                    finally
                    {
                        jsonObj.put("rc", rc);
                    }

                    if (rc == 0)
                        jsonObj.put("results", logResults(stmt, isRS));
                    else
                        jsonObj.put("results", new JSONArray());
                    jsonObj.put("error", sqlEx);

                }
                catch (Exception e)
                {
                    e.printStackTrace();
                }
                finally
                {
                    if (stmt != null)
                        stmt.close();
                }
            }
            DsQueryCtrl.println(jsonArr.toJSONString());
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    /**
     * TODO: logResults definition.
     * 
     * @param stmt
     * @param isRS
     * @return
     * @throws SQLException
     */
    @SuppressWarnings("unchecked")
    private static JSONArray logResults(Statement stmt, boolean isRS)
            throws SQLException
    {
        int result = 1;
        JSONArray json = new JSONArray();

        while (isRS || stmt.getUpdateCount() > -1)
        {
            if (isRS)
            {
                ResultSet rs = null;

                try
                {
                    rs = stmt.getResultSet();
                    json.add(logResultsetResult(rs));
                }
                finally
                {
                    if (rs != null)
                    {
                        rs.close();
                        rs = null;
                    }
                }
            }
            else
            {
                int updateCount = stmt.getUpdateCount();
                logUpdateCount(updateCount);
            }

            isRS = stmt.getMoreResults();
            result++;
        }
        return json;
    }

    /**
     * TODO: logResultsetResult definition.
     * 
     * @param rs
     * @throws SQLException
     */
    @SuppressWarnings("unchecked")
    private static JSONArray logResultsetResult(ResultSet rs)
            throws SQLException
    {
        JSONArray json = new JSONArray();
        if (rs != null)
            while (rs.next())
            {
                json.add(logRow(rs));
            }
        return json;
    }

    /**
     * TODO: logUpdateCount definition.
     * 
     * @param updateCount
     */
    @SuppressWarnings("unchecked")
    private static JSONObject logUpdateCount(int updateCount)
    {
        JSONObject json = new JSONObject();
        json.put("rowcount", updateCount);
        return json;
    }

    /**
     * TODO: logRow definition.
     * 
     * @param rs
     * @throws SQLException
     */
    private static LinkedHashMap<String, Object> logRow(ResultSet rs)
            throws SQLException
    {
        LinkedHashMap<String, Object> json = new LinkedHashMap<String, Object>();
        for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++)
        {
            json.put(rs.getMetaData().getColumnLabel(i), rs.getObject(i));
        }
        return json;
    }

    protected static void printHelp()
    {
        println("Query Utility");
        println("Syntax:  query {-url <jdbc_url> [-user <jdbc_user>] [-password] | -conf <path_to_file>} [-file <path_to_sql_file>] ");
        println("Options:");
        println("  -url <jdbc_url>              - JDBC url of the database to connect to");
        println("  -user <jdbc_user>            - User used to connect to the database");
        println("  -password                    - Prompt for password");
        println("");
        println("  -conf <path_to_file>         - Configuration file that contains values for connection properties (url, user and password)");
        println("  -file <path_to_sql_file>    - File containing the SQL commands to run.");
        println("                                 If missing, read SQL commands from STDIN");
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
