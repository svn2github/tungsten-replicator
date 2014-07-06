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
    public static void main(String argv[])
    {
        try
        {
            // Command line parameters and options.
            String configFile = null;
            // String command = null;
            String fileName = null;
            String user = null, password = null, url = null;

            // Parse command line arguments.
            ArgvIterator argvIterator = new ArgvIterator(argv);
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
                    System.out.print("Enter password: ");
                    password = new String(System.console().readPassword());
                }
                else if ("-url".equals(curArg))
                {
                    url = argvIterator.next();
                }
                else if (curArg.startsWith("-"))
                    fatal("Unrecognized option: " + curArg, null);
                // else
                // command = curArg;
            }

            if (configFile != null)
            {
                TungstenProperties props = new TungstenProperties();
                props.load(new FileInputStream(new File(configFile)));

                if (user == null)
                    user = props.getString("user", "", true);

                if (password == null)
                    password = props.getString("password", "", true);

                if (url == null)
                    url = props.getString("url", "", true);
            }

            BufferedReader br = null;

            boolean readingFromStdIn = false;
            if (fileName == null)
            {
                readingFromStdIn = true;
                br = new BufferedReader(new InputStreamReader(System.in));
            }
            else
            {
                br = new BufferedReader(new FileReader(new File(fileName)));
            }

            Database database = DatabaseFactory.createDatabase(url, user,
                    password);

            database.connect();

            String sql = null;

            SQLException sqlEx;
            StringBuilder output = new StringBuilder();
            output.append('[');
            int queryNum = 1;

            while ((sql = br.readLine()) != null)
            {
                sqlEx = null;
                sql = sql.trim();
                if (readingFromStdIn && sql.length() == 0)
                    break;
                else if (sql.startsWith("#") || sql.length() == 0)
                    continue;
                if (queryNum > 1)
                    output.append(",\n");
                output.append('{');
                Statement stmt = null;
                int rc = 0;
                try
                {
                    stmt = database.createStatement();

                    boolean isRS = false;

                    logStatement(output, sql);

                    output.append(',');

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
                        logRC(output, rc);
                    }

                    output.append(',');
                    if (rc == 0)
                        logResults(output, stmt, isRS);
                    else
                        logEmptyResults(output);
                    output.append(',');
                    logError(output, sqlEx);
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
                queryNum++;
                output.append('}');
            }
            output.append(']');
            DsQueryCtrl.println(output.toString());
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    private static void logEmptyResults(StringBuilder output)
    {
        output.append("\"results\":[");
        output.append(']');
    }

    private static void logError(StringBuilder output, SQLException sqlEx)
    {
        output.append("\"error\":");
        if (sqlEx != null)
        {
            output.append('\"');
            output.append(sqlEx.toString());
            output.append('\"');
        }
        else
            output.append("\"\"");
    }

    /**
     * TODO: logResults definition.
     * 
     * @param output
     * @param stmt
     * @param isRS
     * @throws SQLException
     */
    private static void logResults(StringBuilder output, Statement stmt,
            boolean isRS) throws SQLException
    {
        int result = 1;
        output.append("\"results\":[");
        while (isRS || stmt.getUpdateCount() > -1)
        {
            if (result > 1)
                output.append(",\n");

            if (isRS)
            {
                ResultSet rs = null;

                try
                {
                    rs = stmt.getResultSet();
                    logResultsetResult(output, rs);
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
                logUpdateCount(output, updateCount);
            }

            isRS = stmt.getMoreResults();
            result++;
        }
        output.append(']');
    }

    /**
     * TODO: logResultsetResult definition.
     * 
     * @param output
     * @param rs
     * @throws SQLException
     */
    private static void logResultsetResult(StringBuilder output, ResultSet rs)
            throws SQLException
    {
        output.append('[');
        boolean firstRowDone = false;
        if (rs != null)
            while (rs.next())
            {
                if (firstRowDone)
                {
                    output.append(",\n");
                }
                else
                    firstRowDone = true;
                logRow(output, rs);
            }
        output.append(']');
    }

    /**
     * TODO: logUpdateCount definition.
     * 
     * @param output
     * @param updateCount
     */
    private static void logUpdateCount(StringBuilder output, int updateCount)
    {
        output.append('{');
        output.append("\"rowcount\":");
        output.append(updateCount);
        output.append('}');
    }

    /**
     * TODO: logRow definition.
     * 
     * @param output
     * @param rs
     * @throws SQLException
     */
    private static void logRow(StringBuilder output, ResultSet rs)
            throws SQLException
    {
        output.append('{');
        for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++)
        {
            if (i > 1)
                output.append(",");
            output.append('\"');
            output.append(rs.getMetaData().getColumnLabel(i));
            output.append('\"');
            output.append(':');
            Object obj = rs.getObject(i);
            if (rs.wasNull())
                output.append("null");
            else
            {
                output.append('\"');
                String out = obj.toString().replaceAll("\"", "\\\\\"");
                output.append(out);
                output.append('\"');
            }
        }
        output.append('}');
    }

    /**
     * TODO: logRC definition.
     * 
     * @param output
     * @param rc
     */
    private static void logRC(StringBuilder output, int rc)
    {
        output.append("\"rc\":");
        output.append(rc);
    }

    /**
     * TODO: logStatement definition.
     * 
     * @param output
     * @param sql
     */
    private static void logStatement(StringBuilder output, String sql)
    {
        output.append("\"statement\":");
        output.append('\"');
        output.append(sql);
        output.append('\"');
    }

    protected static void printHelp()
    {
        println("Not implemented");
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
