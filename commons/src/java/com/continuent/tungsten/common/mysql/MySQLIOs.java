/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2010 Continuent Inc.
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
 * Initial developer(s): Gilles Rayrat
 * Contributor(s): 
 */

package com.continuent.tungsten.common.mysql;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.cluster.resource.ResourceState;
import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.common.config.cluster.ConfigurationException;
import com.continuent.tungsten.common.config.cluster.QueryStatusToResourceStateConfiguration;

/**
 * Utility class to retrieve the input and output streams of a JDBC Connection
 * to a MySQL server. These IOs serve the mean of fast and direct interactions
 * with the server (also known as pass-through mode).<br>
 * Current implementation allows IOs retrieval for both MySQL and Drizzle
 * drivers even when behind the SQL-Router or a c3p0 connection pool
 * 
 * @author <a href="gilles.rayrat@continuent.com">Gilles Rayrat</a>
 */
public class MySQLIOs
{
    private final static String                            TSR_CONNECTION_SIMPLE_CLASSNAME   = "TSRConnection";
    private final static String                            TSR_CONNECTION_FULL_CLASSNAME     = "com.continuent.tungsten.router.jdbc.TSRConnection";
    private final static String                            C3P0_CONNECTION_CLASSNAME         = "NewProxyConnection";
    private final static String                            C3P0_INNER_CONNECTION_VARNAME     = "inner";
    private final static String                            BONECP_CONNECTION_CLASSNAME       = "ConnectionHandle";
    private final static String                            BONECP_INNER_CONNECTION_VARNAME   = "connection";
    private final static String                            MYSQL_CONNECTION_CLASSNAME_PREFIX = "com.mysql.jdbc";
    private final static String                            MYSQL_CONNECTION_CLASSNAME        = MYSQL_CONNECTION_CLASSNAME_PREFIX
                                                                                                     + ".ConnectionImpl";
    private final static String                            MYSQL_CONNECTION_CLASSNAME_5_0    = MYSQL_CONNECTION_CLASSNAME_PREFIX
                                                                                                     + ".Connection";
    private final static String                            DRIZZLE_CONNECTION_CLASSNAME      = "org.drizzle.jdbc.DrizzleConnection";
    private static final Logger                            logger                            = Logger.getLogger(MySQLIOs.class);

    /** Where we will read data sent by the MySQL server */
    private final InputStream                              input;

    /** Where we will write data to the MySQL server */
    private final BufferedOutputStream                     output;

    public static final String                             STATUS_KEY                        = "Status";
    public static final String                             STATUS_EXCEPTION                  = "Exception";
    public static final String                             MYSQL_ERRNO                       = "MySQLErrNo";
    public static final String                             MYSQL_SQL_STATE                   = "MySQLState";
    public static final String                             STATUS_MESSAGE_KEY                = "StatusMsg";
    public static final String                             RESULT_KEY                        = "Result";
    public static final String                             TIME_TO_CONNECT_MS                = "TimeToConnectMs";
    public static final String                             TIME_TO_INIT_MS                   = "TimeToInitMs";
    public static final String                             TIME_TO_EXEC_QUERY_MS             = "TimeToExecQueryMs";

    public static final String                             SOCKET_PHASE_CONNECT              = "connecting to";
    public static final String                             SOCKET_PHASE_READ                 = "reading from";
    public static final String                             SOCKET_PHASE_WRITE                = "writing to";

    private static QueryStatusToResourceStateConfiguration stateMapConfig                    = null;

    public enum ExecuteQueryStatus
    {
        OK, TOO_MANY_CONNECTIONS, OPEN_FILE_LIMIT_ERROR, SOCKET_NO_IO, SOCKET_CONNECT_TIMEOUT, SEND_QUERY_TIMEOUT, QUERY_RESULTS_TIMEOUT, QUERY_EXEC_TIMEOUT, LOGIN_RESPONSE_TIMEOUT, QUERY_TOO_LARGE, QUERY_RESULT_FAILED, QUERY_EXECUTION_FAILED, SOCKET_IO_ERROR, MYSQL_ERROR, UNEXPECTED_EXCEPTION, MYSQL_PREMATURE_EOF, HOST_IS_DOWN, NO_ROUTE_TO_HOST, UNTRAPPED_CONDITION
    }

    /**
     * Constructor for internal use only - interface for retrieving IOs is
     * {@link #getMySQLIOs(Connection)}
     * 
     * @param in where to read mysql server data from
     * @param out buffered stream for writing data to mysql server
     */
    private MySQLIOs(InputStream in, BufferedOutputStream out)
    {
        input = in;
        output = out;
    }

    /**
     * Extracts MySQL server input and output streams from the given connection. <br>
     * In order to avoid explicit driver dependency, this function uses
     * introspection to retrieve the connection inner field
     * 
     * @param connection a jdbc connection object that must be connected to a
     *            MySQL server
     * @return a new MySQLIOs object containing extracted input and output
     *         streams of the given connection
     * @throws IOException if the streams could not be extracted
     */
    public static MySQLIOs getMySQLIOs(Connection connection)
            throws IOException
    {
        String className;
        Object realConnection = extractInnerConnection(connection);

        if (realConnection == null)
        {
            throw new IOException(
                    "Could not get MySQL connection I/Os because inner connection is null");
        }

        // Here we know that realConnection is not null
        className = realConnection.getClass().getName();

        try
        {
            // MySQL connection IO extraction: need to get "io" field of
            // connection then inner in and output streams
            if (className.startsWith(MYSQL_CONNECTION_CLASSNAME_PREFIX))
            {
                Object ioObj = getMySQLConnectionIOField(realConnection);
                if (ioObj == null)
                {
                    // IOs already closed
                    return null;
                }
                return new MySQLIOs((InputStream) getFieldFromMysqlIO(ioObj,
                        "mysqlInput"),
                        (BufferedOutputStream) getFieldFromMysqlIO(ioObj,
                                "mysqlOutput"));
            }
            // Drizzle connection hold its i/Os in the "protocol" member
            // variable
            else if (className.equals(DRIZZLE_CONNECTION_CLASSNAME))
            {
                Class<?> implClazz = realConnection.getClass();
                Field protocolField = implClazz.getDeclaredField("protocol");
                protocolField.setAccessible(true);
                Object protocolObj = protocolField.get(realConnection);
                return new MySQLIOs(
                        getDrizzleConnectionInputStream(protocolObj),
                        (BufferedOutputStream) getDrizzleConnectionOutputStream(protocolObj));
            }
            else if (className.equals(TSR_CONNECTION_FULL_CLASSNAME))
            {
                return getMySQLIOs((Connection) realConnection);
            }
            else
            {
                throw new IOException("Unknown connection type " + className
                        + ". Cannot retrieve inner I/Os");
            }
        }
        catch (Exception e)
        {
            logger.error("Couldn't get connection IOs", e);
            throw new IOException(e.getLocalizedMessage());
        }
    }

    private static Object getMySQLConnectionIOField(Object realConnection)
            throws ClassNotFoundException, NoSuchFieldException,
            IllegalAccessException
    {
        String className = realConnection.getClass().getName();
        Class<?> implClazz = null;
        if (MYSQL_CONNECTION_CLASSNAME_5_0.equals(className))
        {
            implClazz = Class.forName(MYSQL_CONNECTION_CLASSNAME_5_0);
        }
        else
        {
            // with java 6, we'll get a JDBC4Connection which needs to
            // be down-casted to a ConnectionImpl
            implClazz = Class.forName(MYSQL_CONNECTION_CLASSNAME);
        }
        Field ioField = implClazz.getDeclaredField("io");
        ioField.setAccessible(true);
        Object ioObj = ioField.get(implClazz.cast(realConnection));
        return ioObj;
    }

    /**
     * A pooled or SQL-Router JDBC connection wraps the original MySQL
     * connection object. This function allows to extract the MySQL connection
     * object from a given Connection, regardless of the implementing class
     * 
     * @param connection a regular, pooled or SQL-Router connection
     * @return the wrapped MySQL connection as an object, which can be either
     *         MySQL jdbc3 or jdbc4 connection
     * @throws IOException upon problems getting the inner connection
     */
    public static Object extractInnerConnection(Connection connection)
            throws IOException
    {
        if (connection == null)
        {
            return null;
        }
        Object realConnection = connection;
        String className = realConnection.getClass().getSimpleName();

        // First, we need to get to the real, inner MySQL connection that is
        // possibly wrapped by the connection we received
        // Possible stacks:
        // 1/ router->MySQL
        // 2/ router->c3p0->MySQL
        // 3/ c3p0->MySQL
        // 4/ c3p0->router->MySQL

        if (TSR_CONNECTION_SIMPLE_CLASSNAME.equals(className))
        {
            realConnection = extractInnerConnectionFromSQLR(realConnection);
            if (realConnection != null)
                className = realConnection.getClass().getSimpleName();
        }
        if (C3P0_CONNECTION_CLASSNAME.equals(className))
        {
            realConnection = extractInnerConnectionFromC3P0(realConnection);
            if (realConnection != null)
                className = realConnection.getClass().getSimpleName();
        }
        else if (BONECP_CONNECTION_CLASSNAME.equals(className))
        {
            realConnection = extractInnerConnectionFromBoneCP(realConnection);
            if (realConnection != null)
                className = realConnection.getClass().getSimpleName();
        }
        // loop one more time in case this is a c3p0->router stack
        if (TSR_CONNECTION_SIMPLE_CLASSNAME.equals(className))
        {
            realConnection = extractInnerConnectionFromSQLR(realConnection);
            if (realConnection != null)
                className = realConnection.getClass().getSimpleName();
        }
        return realConnection;
    }

    /**
     * Given a C3P0 pooled connection, extracts the enclosed "real" connection
     * 
     * @param pooledConnection a c3p0-pooled connection
     * @param memberVariableName name of the inner connection variable
     * @return JDBC connection wrapped by the given connection
     * @throws IOException if an error occurs retrieving inner connection
     */
    public static Object extractInnerConnectionFromC3P0(Object pooledConnection)
            throws IOException
    {
        return extractInnerConnectionFromPooledConnection(pooledConnection,
                C3P0_INNER_CONNECTION_VARNAME);
    }

    /**
     * Given a BoneCP pooled connection, extracts the enclosed "real" connection
     * 
     * @param pooledConnection a boneCP-pooled connection
     * @param memberVariableName name of the inner connection variable
     * @return JDBC connection wrapped by the given connection
     * @throws IOException if an error occurs retrieving inner connection
     */
    public static Object extractInnerConnectionFromBoneCP(
            Object pooledConnection) throws IOException
    {
        return extractInnerConnectionFromPooledConnection(pooledConnection,
                BONECP_INNER_CONNECTION_VARNAME);
    }

    /**
     * Given a connection pool connection, extracts the enclosed "real"
     * connection
     * 
     * @param pooledConnection a c3p0 or boneCP -pooled connection
     * @param memberVariableName name of the inner connection variable
     * @return JDBC connection wrapped by the given connection
     * @throws IOException if an error occurs retrieving inner connection
     */
    public static Object extractInnerConnectionFromPooledConnection(
            Object pooledConnection, String memberVariableName)
            throws IOException
    {
        if (logger.isTraceEnabled())
            logger.trace("Getting pooled connection inner connection");
        try
        {
            Field connectionField = pooledConnection.getClass()
                    .getDeclaredField(memberVariableName);
            connectionField.setAccessible(true);
            pooledConnection = (Connection) connectionField
                    .get(pooledConnection);
        }
        catch (Exception e)
        {
            throw new IOException(e.getLocalizedMessage());
        }
        return pooledConnection;
    }

    /**
     * Given a SQL-Router connection, retrieve the encapsulated connection
     * 
     * @param sqlrConnection Tungsten SQL-router connection
     * @return JDBC connection wrapped by the given router connection
     * @throws IOException if an error occurs retrieving inner connection
     */
    public static Object extractInnerConnectionFromSQLR(Object sqlrConnection)
            throws IOException
    {
        if (logger.isTraceEnabled())
            logger.trace("Getting SQL-Router inner connection");
        try
        {
            Field connectionField = sqlrConnection.getClass().getDeclaredField(
                    "realConnection");
            connectionField.setAccessible(true);
            sqlrConnection = connectionField.get(sqlrConnection);
        }
        catch (Exception e)
        {
            throw new IOException(e.getLocalizedMessage());
        }
        return sqlrConnection;
    }

    /**
     * Uses java introspection to retrieve the given field from the MysqlIO
     * object passed
     * 
     * @param fieldName class field name to retrieve
     * @param io the connection I/O field
     * @return the input stream of the connected mysql server
     * @throws IOException upon error while getting object
     */
    private static Object getFieldFromMysqlIO(Object io, String fieldName)
            throws IOException
    {
        try
        {
            Field f = io.getClass().getDeclaredField(fieldName);
            f.setAccessible(true);
            return f.get(io);
        }
        catch (Exception e)
        {
            throw new IOException(e.getLocalizedMessage());
        }
    }

    /**
     * Extracts the Drizzle JDBC connection input stream to the MySQL server
     * 
     * @param protocolObj the field "protocol" of the Drizzle Connection object
     * @return an InputStream used to communicate with the connected MySQL
     *         server
     * @throws IOException upon error getting the appropriate field
     */
    private static InputStream getDrizzleConnectionInputStream(
            Object protocolObj) throws IOException
    {
        try
        {
            Field packetFetcherField = protocolObj.getClass().getDeclaredField(
                    "packetFetcher");
            packetFetcherField.setAccessible(true);
            Object packetFetcherObj = packetFetcherField.get(protocolObj);
            Field inputStreamField = packetFetcherObj.getClass()
                    .getDeclaredField("inputStream");
            inputStreamField.setAccessible(true);
            return (InputStream) inputStreamField.get(packetFetcherObj);
        }
        catch (Exception e)
        {
            throw new IOException(e.getLocalizedMessage());
        }
    }

    /**
     * Extracts the Drizzle JDBC connection output stream of the MySQL server
     * 
     * @param protocolObj the field "protocol" of the Drizzle Connection object
     * @return an OutputStream used to communicate with the connected MySQL
     *         server
     * @throws IOException upon error getting the appropriate field
     */
    private static OutputStream getDrizzleConnectionOutputStream(
            Object protocolObj) throws IOException
    {
        try
        {
            Field writerField = protocolObj.getClass().getDeclaredField(
                    "writer");
            writerField.setAccessible(true);
            return (OutputStream) writerField.get(protocolObj);
        }
        catch (Exception e)
        {
            throw new IOException(e.getLocalizedMessage());
        }
    }

    /**
     * Every connection to a MySQL server has a server side ID, called
     * connection ID or server thread ID. This function allows getting this ID,
     * currently only on a MySQL driver connection<br>
     * TODO: implement Drizzle version
     * 
     * @param connection the (connected) connection to get ID from
     * @return the server thread ID of the given connection as a long
     */
    public static long getServerThreadID(Connection connection)
    {
        String className;
        try
        {
            Object realConnection = extractInnerConnection(connection);

            if (realConnection == null)
            {
                return 0;
            }

            // Here we know that realConnection is not null
            className = realConnection.getClass().getName();

            // MySQL connection IO extraction: need to get "io" field of
            // connection then inner in and output streams
            if (className.startsWith(MYSQL_CONNECTION_CLASSNAME_PREFIX))
            {
                Object ioObj = getMySQLConnectionIOField(realConnection);
                if (ioObj == null)
                {
                    // IOs already closed
                    return 0;
                }
                return (Long) getFieldFromMysqlIO(ioObj, "threadId");
            }
        }
        catch (Exception e)
        {
            logger.error("Couldn't get connection server thread ID", e);
        }
        return 0;
    }

    /**
     * Tells whether the given connection is one that we can exploit for the
     * purposes of getting access to MySQL IOs.<br>
     * 
     * @param conn connection to test
     * @return true if the connection is one of c3p0, SQL-Router, MySQL
     *         connector/j or Drizzle connector. Otherwise false.
     */
    public static boolean connectionIsCompatible(Connection conn)
    {
        if (conn == null)
            return false;
        String className = conn.getClass().getSimpleName();

        if (className.equals(C3P0_CONNECTION_CLASSNAME)
                || className.equals(BONECP_CONNECTION_CLASSNAME)
                || className.equals(DRIZZLE_CONNECTION_CLASSNAME)
                || className.equals(TSR_CONNECTION_SIMPLE_CLASSNAME)
                || conn.getClass().getName()
                        .startsWith(MYSQL_CONNECTION_CLASSNAME_PREFIX))
            return true;

        return false;
    }

    public InputStream getInput()
    {
        return input;
    }

    public BufferedOutputStream getOutput()
    {
        return output;
    }

    /**
     * Checks a MySQL server connectivity by running the given query against it.
     * This function differs for simple JDBC driver connect/execute in the sense
     * that it sets timeout on sockets, so that if the server is unresponsive,
     * the function will return after the timeout expires. If the server is not
     * accepting connections because of "Too many connections error" or if the
     * socket timeout expires, this function will consider the MySQL server as
     * alive (or potentially alive) and return true. For finer-grain diagnosis
     * or request result retrieval, use
     * {@link #executeQueryWithTimeouts(String, int, String, String, String, String, int)}
     * 
     * @param hostname
     * @param port
     * @param user
     * @param password
     * @param db null or empty when not wanting to connect to any DB
     * @param query the query to be executed against the server - only selects
     *            are supported
     * @param timeoutMsecs must be positive. Zero timeout is considered as no
     *            timeout
     * @return true if the server is up and running, false otherwise
     */
    public static boolean isAlive(String hostname, int port, String user,
            String password, String db, String query, int timeoutMsecs)
    {
        try
        {

            TungstenProperties tp = executeQueryWithTimeouts(hostname, port,
                    user, password, db, query, timeoutMsecs);

            if (logger.isDebugEnabled())
            {
                logger.debug("Received the following ExecuteQueryStatus: " + tp);
            }

            if (tp == null)
            {
                // something went really wrong, report it
                logger.error("Got a null result while executing executeQueryWithTimeouts!, returning false");
                return false;
            }

            Object statusObj = tp.getObject(STATUS_KEY);

            if (statusObj == null)
            {
                // something went really wrong, report it
                logger.error("Got a null status while executing executeQueryWithTimeouts!, returning false");
                return false;
            }

            return (MySQLIOs
                    .getStateFromQueryStatus((ExecuteQueryStatus) statusObj) != ResourceState.STOPPED);

        }
        catch (Exception err)
        {
            logger.error("Got unexpected exception. Returning true: " + err);
        }
        return true;
    }

    /**
     * Tries to establish a connection to a MySQL server with given credentials
     * and timeout. Upon success, runs the given request (which must be a
     * select!) and returns the result as TungstenProperties holding only the
     * first row, with column names as keys and results as values.
     * 
     * @param hostname
     * @param port
     * @param user
     * @param password
     * @param db null or empty when not wanting to connect to any DB
     * @param query the query to be executed against the server - only selects
     *            are supported
     * @param timeoutMsecs must be positive. Zero timeout is considered as no
     *            timeout
     * @return null if the server is not alive/accessible, or the query result
     *         wrapped inside tungsten properties
     */
    public static TungstenProperties executeQueryWithTimeouts(String hostname,
            int port, String user, String password, String db, String query,
            int timeoutMsecs)
    {
        String statusMessage = null;
        int mysqlErrno = -1;

        TungstenProperties statusAndResult = new TungstenProperties();

        if (query.length() + 1 > MySQLPacket.MAX_LENGTH)
        {
            statusMessage = String.format(
                    "The query size, %d, is too large to execute.",
                    query.length());

            statusAndResult.setObject(STATUS_KEY,
                    ExecuteQueryStatus.QUERY_TOO_LARGE);
            statusAndResult.setString(STATUS_MESSAGE_KEY, statusMessage);
            logger.warn(formatExecStatus(statusAndResult));
            return logAndReturnProperties(statusAndResult);
        }

        Socket socket = null;

        String socketPhase = SOCKET_PHASE_CONNECT;
        ExecuteQueryStatus timeoutPhase = ExecuteQueryStatus.SOCKET_CONNECT_TIMEOUT;

        try
        {
            /*
             * The following call can block for many seconds, before returning,
             * if the host specified is not up or doesn't exist. Be forewarned.
             */
            SocketAddress sockaddr = new InetSocketAddress(hostname, port);

            // Create the socket without connecting yet...
            socket = new Socket();

            // ... because we want to set a connection timeout first
            socket.setSoTimeout(timeoutMsecs);

            /*
             * This can be called fairly frequently, and we want the FD used to
             * be reclaimed quickly.
             */
            socket.setReuseAddress(true);

            long beforeConnect = System.currentTimeMillis();

            // CONNECT AT SOCKET LEVEL
            // now do connect, without forgetting to set the connect timeout
            socket.connect(sockaddr, timeoutMsecs);

            long timeToConnectMs = System.currentTimeMillis() - beforeConnect;
            if (logger.isTraceEnabled())
            {
                logger.trace("Connection to " + hostname + " took "
                        + timeToConnectMs + "ms");
            }

            statusAndResult.setLong(TIME_TO_CONNECT_MS, timeToConnectMs);
            InputStream socketInput = socket.getInputStream();
            OutputStream socketOutput = socket.getOutputStream();

            if (socketInput == null || socketOutput == null)
            {
                statusMessage = String
                        .format("Socket connect error: InputStream=%s, OutputStream=%s after connect to %s:%s",
                                socketInput, socketOutput, hostname, port);

                statusAndResult.setObject(STATUS_KEY,
                        ExecuteQueryStatus.SOCKET_NO_IO);
                statusAndResult.setString(STATUS_MESSAGE_KEY, statusMessage);
                logger.warn(formatExecStatus(statusAndResult));
                return logAndReturnProperties(statusAndResult);
            }

            int timeLeft = (int) (timeoutMsecs - timeToConnectMs);
            if (timeLeft <= 0)
            {
                statusMessage = String
                        .format("Timeout while connecting: %d ms exceeds allowed timeout of %d ms.",
                                timeToConnectMs, timeoutMsecs);

                statusAndResult.setObject(STATUS_KEY,
                        ExecuteQueryStatus.SOCKET_CONNECT_TIMEOUT);
                statusAndResult.setString(STATUS_MESSAGE_KEY, statusMessage);
                logger.warn(formatExecStatus(statusAndResult));
                return logAndReturnProperties(statusAndResult);
            }

            // Reset the timeout to what time we have left. This will be used in
            // the read cycle.
            socket.setSoTimeout(timeLeft);
            long beforeInit = System.currentTimeMillis();

            socketPhase = SOCKET_PHASE_READ;
            timeoutPhase = ExecuteQueryStatus.LOGIN_RESPONSE_TIMEOUT;

            // get the welcome packet and extract the seed
            MySQLPacket greeting = MySQLPacket.mysqlReadPacket(socketInput,
                    true);
            if (greeting.isError())
            {
                greeting.getByte();
                mysqlErrno = greeting.getShort();

                statusAndResult.setInt(MYSQL_ERRNO, mysqlErrno);

                if ((int) mysqlErrno == MySQLConstants.ER_CON_COUNT_ERROR)
                {
                    statusMessage = String.format(
                            "MySQL Error # %d, Too many connections",
                            mysqlErrno);

                    statusAndResult.setObject(STATUS_KEY,
                            ExecuteQueryStatus.TOO_MANY_CONNECTIONS);
                    statusAndResult
                            .setString(STATUS_MESSAGE_KEY, statusMessage);
                    logger.warn(formatExecStatus(statusAndResult));
                    return logAndReturnProperties(statusAndResult);
                }
                else
                {
                    statusMessage = String.format("MySQL Error # %d",
                            mysqlErrno);

                    statusAndResult.setObject(STATUS_KEY,
                            ExecuteQueryStatus.MYSQL_ERROR);
                    statusAndResult
                            .setString(STATUS_MESSAGE_KEY, statusMessage);
                    logger.warn(formatExecStatus(statusAndResult));
                    return logAndReturnProperties(statusAndResult);
                }
            }

            byte[] seed = MySQLGreetingPacket.getSeed(greeting);
            // create the encrypted password string
            byte[] encryptedPassword = encryptMySQLPassword(password, seed);
            // create the authentication packet
            MySQLAuthPacket auth = new MySQLAuthPacket(
                    (byte) (greeting.getPacketNumber() + 1), user,
                    encryptedPassword, db);

            // and send it
            socketPhase = SOCKET_PHASE_WRITE;
            timeoutPhase = ExecuteQueryStatus.SEND_QUERY_TIMEOUT;
            auth.write(socketOutput);
            socketOutput.flush();

            socketPhase = SOCKET_PHASE_READ;
            timeoutPhase = ExecuteQueryStatus.QUERY_EXEC_TIMEOUT;
            // Now, get the server answer
            MySQLPacket response = MySQLPacket.mysqlReadPacket(socketInput,
                    true);
            if (response.isOK())
            {
                if (logger.isDebugEnabled())
                {
                    logger.debug("Connected!");
                }
                long timeToInitMs = System.currentTimeMillis() - beforeInit;
                if (logger.isTraceEnabled())
                {
                    logger.trace("Took " + timeToInitMs
                            + "ms to initialize database connection");
                }
                statusAndResult.setLong(TIME_TO_INIT_MS, timeToInitMs);
                timeLeft = (int) (timeLeft - timeToInitMs);
                if (timeLeft <= 0)
                {
                    statusMessage = String
                            .format("Socket connect + login attempt took %d ms which exceeds allowed timeout of %d ms.",
                                    (timeToConnectMs + timeToInitMs),
                                    timeoutMsecs);
                    statusAndResult.setObject(STATUS_KEY,
                            ExecuteQueryStatus.LOGIN_RESPONSE_TIMEOUT);
                    statusAndResult
                            .setString(STATUS_MESSAGE_KEY, statusMessage);
                    return logAndReturnProperties(statusAndResult);
                }

                long beforeQuery = System.currentTimeMillis();
                socket.setSoTimeout((int) (timeoutMsecs - timeToConnectMs - timeToInitMs));
                MySQLQueryPacket queryPacket = new MySQLQueryPacket((byte) 0,
                        query);

                timeoutPhase = ExecuteQueryStatus.QUERY_RESULTS_TIMEOUT;

                socketPhase = SOCKET_PHASE_WRITE;
                queryPacket.write(socketOutput);
                socketOutput.flush();

                socketPhase = SOCKET_PHASE_READ;
                MySQLPacket queryResult = MySQLPacket.mysqlReadPacket(
                        socketInput, true);
                long timeToExecQueryMs = System.currentTimeMillis()
                        - beforeQuery;

                if (logger.isTraceEnabled())
                {
                    logger.trace("Took " + timeToExecQueryMs
                            + "ms to run query " + query);
                }

                statusAndResult.setLong(TIME_TO_EXEC_QUERY_MS,
                        timeToExecQueryMs);

                timeLeft = (int) (timeLeft - timeToExecQueryMs);

                if (timeLeft <= 0)
                {
                    statusMessage = String
                            .format("Connection + database initialization + query execution took %d ms which exceeds allowed timeout of %d ms.",
                                    (timeToConnectMs + timeToInitMs + timeToExecQueryMs),
                                    timeoutMsecs);

                    statusAndResult.setObject(STATUS_KEY,
                            ExecuteQueryStatus.QUERY_EXEC_TIMEOUT);
                    statusAndResult
                            .setString(STATUS_MESSAGE_KEY, statusMessage);
                    logger.warn(formatExecStatus(statusAndResult));
                    return logAndReturnProperties(statusAndResult);
                }

                if (queryResult.isError())
                {
                    if (logger.isDebugEnabled())
                    {
                        logger.debug("Result was: " + queryResult);
                    }
                    queryResult.getByte(); // Error marker, always 0xFF
                    mysqlErrno = queryResult.getShort();
                    queryResult.getByte(); // Sql state marker, always '#'
                    queryResult.getBytes(5); // sql state

                    statusMessage = String
                            .format("Query failed. MySQL Errno: %d, SQL State:%d, '%s'\nQuery: '%s'",
                                    response.peekErrorErrno(), response
                                            .peekErrorSQLState(), query,
                                    queryResult.getString(queryResult
                                            .getRemainingBytes()));

                    statusAndResult.setObject(STATUS_KEY,
                            ExecuteQueryStatus.QUERY_RESULT_FAILED);
                    statusAndResult.setInt(MYSQL_ERRNO, mysqlErrno);

                    statusAndResult
                            .setObject(STATUS_MESSAGE_KEY, statusMessage);
                    logger.warn(formatExecStatus(statusAndResult));
                    return logAndReturnProperties(statusAndResult);
                }

                if (logger.isDebugEnabled())
                {
                    logger.debug("Query " + query + " succeeded");
                }

                int numberOfColumns = queryResult.getByte();
                List<String> columnNames = new ArrayList<String>();
                List<Byte> columnTypes = new ArrayList<Byte>();
                TungstenProperties resultSet = new TungstenProperties();
                for (int i = 0; i < numberOfColumns; i++)
                {
                    // retain only the column name
                    queryResult = MySQLPacket
                            .mysqlReadPacket(socketInput, true);
                    queryResult.getLenEncodedString(true); // catalog
                    queryResult.getLenEncodedString(true); // dbname
                    queryResult.getLenEncodedString(true); // table
                    queryResult.getLenEncodedString(true); // org_table
                    String name = queryResult.getLenEncodedString(true); // name
                    columnNames.add(name);
                    queryResult.getLenEncodedString(true); // org_name
                    queryResult.getByte(); // filler
                    queryResult.getShort(); // charsetnr
                    queryResult.getInt32(); // length
                    byte type = queryResult.getByte(); // type
                    columnTypes.add(type);
                }
                queryResult = MySQLPacket.mysqlReadPacket(socketInput, true);// EOF
                queryResult = MySQLPacket.mysqlReadPacket(socketInput, true);
                for (int i = 0; i < numberOfColumns; i++)
                {
                    String row = queryResult.getLenEncodedString(false);
                    if (logger.isDebugEnabled())
                    {
                        logger.debug("Got Row: " + row);
                    }
                    byte type = columnTypes.get(i);
                    // Time and dates must be converted to long (# of ms since
                    // epoch)
                    if (type == MySQLConstants.MYSQL_TYPE_DATE
                            || type == MySQLConstants.MYSQL_TYPE_DATETIME
                            || type == MySQLConstants.MYSQL_TYPE_NEWDATE
                            || type == MySQLConstants.MYSQL_TYPE_TIMESTAMP)
                    {
                        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
                        // timestamps that contain hour info
                        if (row.length() > 11)
                        {
                            f = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        }
                        try
                        {
                            java.util.Date d = f.parse(row);
                            resultSet.setLong(columnNames.get(i), d.getTime());
                        }
                        catch (ParseException pe)
                        {
                            // Don't throw an error but keep it safe:
                            resultSet.setLong(columnNames.get(i), 0L);
                        }
                    }
                    else
                    {
                        resultSet.setString(columnNames.get(i), row);
                    }
                }

                statusMessage = String.format("Query to %s:%d succeeded.",
                        hostname, port);
                statusAndResult.setString(STATUS_MESSAGE_KEY, statusMessage);
                statusAndResult.setObject(STATUS_KEY, ExecuteQueryStatus.OK);
                statusAndResult.setObject(RESULT_KEY, resultSet);
                // Clean-up
                columnNames.clear();
                columnTypes.clear();
                return logAndReturnProperties(statusAndResult);
            }
            else if (response.isEOF())
            {
                statusMessage = String
                        .format("Got MySQL EOF packet before any results: status=%d, warnings=%d\n"
                                + "Make sure you are using a valid mysql user with correct permissions for this query.\n"
                                + "Also, check for entries in mysql.user with a blank name in the 'user' column.\n"
                                + "Finally, make sure you are not using old_passwords=1 in your my.cnf file.",
                                response.peekEOFServerStatus(),
                                response.peekEOFWarningCount());

                statusAndResult.setObject(STATUS_KEY,
                        ExecuteQueryStatus.MYSQL_PREMATURE_EOF);
                statusAndResult.setString(STATUS_MESSAGE_KEY, statusMessage);
                logger.warn(formatExecStatus(statusAndResult));
                return logAndReturnProperties(statusAndResult);
            }
            else if (response.isError())
            {
                /*
                 * Test by dropping the tungsten user and then re-create it to
                 * clear the error.
                 */
                statusMessage = String
                        .format("Query failed. MySQL Errno: %d, SQL State: %d\n%s\nQuery: %s\n",
                                response.peekErrorErrno(),
                                response.peekErrorSQLState(),
                                response.peekErrorErrorMessage(), query);

                statusAndResult.setInt(MYSQL_ERRNO, response.peekErrorErrno());
                statusAndResult.setObject(STATUS_KEY,
                        ExecuteQueryStatus.QUERY_EXECUTION_FAILED);
                statusAndResult.setObject(STATUS_MESSAGE_KEY, statusMessage);
                logger.warn(formatExecStatus(statusAndResult));
                return logAndReturnProperties(statusAndResult);
            }
        }
        catch (SocketTimeoutException so)
        {
            statusMessage = String.format(
                    "Socket timeout while %s a socket %s:%d\nException='%s'",
                    socketPhase, hostname, port, so);

            statusAndResult.setObject(STATUS_KEY, timeoutPhase);
            statusAndResult.setString(STATUS_MESSAGE_KEY, statusMessage);
            statusAndResult.setObject(STATUS_EXCEPTION, so);
            logger.warn(formatExecStatus(statusAndResult));
            return logAndReturnProperties(statusAndResult);
        }
        catch (IOException ioe)
        {
            if ("Host is down".toLowerCase().contains(
                    ioe.getMessage().toLowerCase()))
            {
                statusMessage = String
                        .format("Host '%s' is down detected while %s a socket to %s:%d\nException='%s'",
                                hostname, socketPhase, hostname, port, ioe);
                statusAndResult.setObject(STATUS_KEY,
                        ExecuteQueryStatus.HOST_IS_DOWN);
                statusAndResult.setString(STATUS_MESSAGE_KEY, statusMessage);
                statusAndResult.setObject(STATUS_EXCEPTION, ioe);
                logger.warn(formatExecStatus(statusAndResult));
                return logAndReturnProperties(statusAndResult);
            }
            if ("No route to host".toLowerCase().contains(
                    ioe.getMessage().toLowerCase()))
            {
                statusMessage = String
                        .format("No route to host '%s' detected while %s a socket to %s:%d\nException='%s'",
                                hostname, socketPhase, hostname, port, ioe);

                statusAndResult.setObject(STATUS_KEY,
                        ExecuteQueryStatus.NO_ROUTE_TO_HOST);
                statusAndResult.setString(STATUS_MESSAGE_KEY, statusMessage);
                statusAndResult.setObject(STATUS_EXCEPTION, ioe);
                logger.warn(formatExecStatus(statusAndResult));
                return logAndReturnProperties(statusAndResult);
            }
            if (ioe.getMessage().toLowerCase()
                    .contains("cannot assign requested address"))
            {
                statusMessage = String
                        .format("I/O exception while %s a socket to %s:%d\nException='%s'\n"
                                + "Your open file limit may be too low.  Check with 'ulimit -n' and increase if necessary.",
                                socketPhase, hostname, port, ioe);
                statusAndResult.setObject(STATUS_KEY,
                        ExecuteQueryStatus.OPEN_FILE_LIMIT_ERROR);
                statusAndResult.setString(STATUS_MESSAGE_KEY, statusMessage);
                statusAndResult.setObject(STATUS_EXCEPTION, ioe);
                logger.warn(formatExecStatus(statusAndResult));
                return logAndReturnProperties(statusAndResult);
            }

            statusMessage = String
                    .format("I/O exception caught while %s a socket to %s:%d\nException='%s'",
                            socketPhase, hostname, port, ioe);

            statusAndResult.setObject(STATUS_KEY,
                    ExecuteQueryStatus.SOCKET_IO_ERROR);
            statusAndResult.setString(STATUS_MESSAGE_KEY, statusMessage);
            statusAndResult.setObject(STATUS_EXCEPTION, ioe);
            logger.warn(formatExecStatus(statusAndResult));
            return logAndReturnProperties(statusAndResult);
        }
        catch (Exception e)
        {
            /*
             * Test - after re-introducing dropped tungsten user, got an
             * exception here.
             */
            statusMessage = String
                    .format("Exception while attempting to execute a query on %s:%d\nException=%s",
                            hostname, port, e);
            statusAndResult.setObject(STATUS_KEY,
                    ExecuteQueryStatus.UNEXPECTED_EXCEPTION);
            statusAndResult.setString(STATUS_MESSAGE_KEY, statusMessage);
            statusAndResult.setObject(STATUS_EXCEPTION, e);
            logger.warn(formatExecStatus(statusAndResult), e);
            return logAndReturnProperties(statusAndResult);
        }
        finally
        {
            try
            {
                // Try to properly close the connection
                MySQLPacket p = new MySQLPacket(1, (byte) 0);
                p.putByte((byte) MySQLConstants.COM_QUIT);
                p.write(socket.getOutputStream());
            }
            catch (Exception ignored)
            {
            }

            if (socket != null)
            {
                try
                {
                    socket.close();
                }
                catch (IOException i)
                {
                    logger.warn("Exception while closing socket", i);
                }
            }
        }

        statusMessage = String
                .format("Returning after query, '%s' on %s:%d due to an untrapped condition.\nCall Continuent Support",
                        query, hostname, port);

        if (statusAndResult != null)
        {
            statusAndResult.setObject(STATUS_KEY,
                    ExecuteQueryStatus.UNTRAPPED_CONDITION);
            statusAndResult.setString(STATUS_MESSAGE_KEY, statusMessage);
            logger.warn(formatExecStatus(statusAndResult));
        }

        return logAndReturnProperties(statusAndResult);
    }

    /**
     * Do a MySQL specific encryption of the given password<br>
     * Algorithm is: <br>
     * stage1_hash = SHA1(password)<br>
     * token = SHA1(scramble + SHA1(stage1_hash)) XOR stage1_hash
     * 
     * @param password the password to scramble
     * @param seed the server seed to encode the password with
     * @return and encrypted password ready to be sent to the server
     */
    public static byte[] encryptMySQLPassword(String password, byte[] seed)
    {
        if (password == null || password.equals(""))
        {
            return new byte[0];
        }
        MessageDigest digest = null;
        try
        {
            digest = MessageDigest.getInstance("SHA-1");
        }
        catch (NoSuchAlgorithmException e)
        {
            logger.error("Cannot encrypt password", e);
            return new byte[0];
        }

        // SHA1(password)
        byte[] stage1_hash = digest.digest(password.getBytes());
        digest.reset();
        // SHA1(stage1_hash)
        byte[] sha1_stage1 = digest.digest(stage1_hash);
        digest.reset();
        // scramble + SHA1(stage1_hash)
        digest.update(seed);
        digest.update(sha1_stage1);
        // SHA1(scramble + SHA1(stage1_hash))
        byte[] finalSha1 = digest.digest();
        // SHA1(scramble + SHA1(stage1_hash)) XOR stage1_hash
        byte[] token = new byte[finalSha1.length];
        for (int i = 0; i < finalSha1.length; i++)
        {
            token[i] = (byte) (stage1_hash[i] ^ finalSha1[i]);
        }
        return token;
    }

    public static void forceClose(Connection conn)
    {
        try
        {
            MySQLIOs io = MySQLIOs.getMySQLIOs(conn);
            if (io != null)
            {
                InputStream mysqlInput = io.getInput();
                OutputStream mysqlOutput = io.getOutput();
                mysqlInput.close();
                mysqlOutput.close();

                /*
                 * Now, the piece de resistance - do the Connection.close so
                 * that internal data structures are cleaned up etc. But do that
                 * so the rollback method isn't called. However, we prepare to
                 * get exceptions here because we closed the input and output
                 * channels.
                 */
                try
                {
                    Object realConnection = conn;
                    realConnection
                            .getClass()
                            .getMethod(
                                    "realClose",
                                    new Class[]{Boolean.TYPE, Boolean.TYPE,
                                            Boolean.TYPE, Throwable.class})
                            .invoke(realConnection,
                                    new Object[]{
                                            Boolean.FALSE,
                                            Boolean.FALSE,
                                            Boolean.FALSE,
                                            new Exception(
                                                    "Forced close by SQL router")});
                }
                catch (Exception expected)
                {
                    if (logger.isInfoEnabled())
                    {
                        logger.info("Got exception Connection.close():\n"
                                + expected);
                    }
                }
            }
            else
            {
                conn.close();
                return;
            }
        }
        catch (Exception e)
        {
            /*
             * We may not be dealing with a MySQL connection. In this case, just
             * delegate the close to the connection.
             */
            try
            {
                conn.close();
            }
            catch (SQLException ignored)
            {
            }
        }
    }

    /**
     * Formats a TungstenProperties from executeQueryWithTimeouts for
     * human-friendly output
     * 
     * @param execStatus
     * @return
     */
    static private String formatExecStatus(TungstenProperties execStatus)
    {

        ExecuteQueryStatus status = (ExecuteQueryStatus) execStatus
                .getObject(STATUS_KEY);
        String statusMessage = execStatus.getString(STATUS_MESSAGE_KEY);

        return String.format("%s\n%s", status.toString(), statusMessage);
    }

    /**
     * Returns the ResourceState that maps to the status of the query execution.
     * 
     * @param status
     * @return
     */
    public static ResourceState getStateFromQueryStatus(
            ExecuteQueryStatus status)
    {
        if (stateMapConfig != null)
        {
            try
            {
                return QueryStatusToResourceStateConfiguration
                        .getMappedState(status);
            }
            catch (ConfigurationException c)
            {
                if (logger.isDebugEnabled())
                {
                    logger.debug(String.format("%s\nUsing default mapping.\n"
                            + "Check the file at %s and correct any issues.",
                            c.getLocalizedMessage(),
                            stateMapConfig.getConfigFileNameInUse()));
                }
            }
        }

        switch (status)
        {
            case OK :
                return ResourceState.ONLINE;

            case SOCKET_CONNECT_TIMEOUT :
            case LOGIN_RESPONSE_TIMEOUT :
            case QUERY_EXEC_TIMEOUT :
            case QUERY_RESULTS_TIMEOUT :
            case SEND_QUERY_TIMEOUT :
                return ResourceState.TIMEOUT;

            case SOCKET_IO_ERROR :
                return ResourceState.STOPPED;

            case HOST_IS_DOWN :
            case NO_ROUTE_TO_HOST :
            case QUERY_TOO_LARGE :
            case OPEN_FILE_LIMIT_ERROR :
            case SOCKET_NO_IO :
            case QUERY_RESULT_FAILED :
            case QUERY_EXECUTION_FAILED :
            case MYSQL_ERROR :
            case UNEXPECTED_EXCEPTION :
            case MYSQL_PREMATURE_EOF :
            case UNTRAPPED_CONDITION :
            case TOO_MANY_CONNECTIONS :
            default :
                return ResourceState.SUSPECT;
        }
    }

    public static void loadStateMappingConfiguration()
            throws ConfigurationException
    {
        stateMapConfig = QueryStatusToResourceStateConfiguration.getInstance();
    }

    public static String showStateMapping()
    {
        StringBuilder builder = new StringBuilder();

        ResourceState mappedState = null;

        for (ExecuteQueryStatus status : ExecuteQueryStatus.values())
        {

            try
            {
                mappedState = QueryStatusToResourceStateConfiguration
                        .getMappedState(status);

                if (mappedState != null)
                {
                    builder.append(String.format("OVERRIDE: %s = %s\n", status,
                            mappedState));

                    continue;
                }
            }
            catch (ConfigurationException c)
            {
                if (logger.isDebugEnabled())
                {
                    logger.debug(String.format("%s\nUsing default mapping.\n"
                            + "Check the file at %s and correct any issues.",
                            c.getLocalizedMessage(),
                            stateMapConfig.getConfigFileNameInUse()));
                }
            }

            builder.append(String.format(" DEFAULT: %s = %s\n", status,
                    getStateFromQueryStatus(status)));
        }

        return builder.toString();
    }

    private static TungstenProperties logAndReturnProperties(
            TungstenProperties props)
    {
        if (logger.isTraceEnabled())
        {
            logger.trace(props);
        }

        return props;
    }
}
