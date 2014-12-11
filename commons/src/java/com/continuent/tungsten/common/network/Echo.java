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
 * Initial developer(s): Csaba Endre Simon
 * Contributor(s): 
 */

package com.continuent.tungsten.common.network;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.config.TungstenProperties;

/**
 * Tests for reachability using the TCP echo protocol.
 * 
 * @author <a href="mailto:csimon@vmware.com">Csaba Endre Simon</a>
 */
public class Echo
{
    private static Logger       logger               = Logger.getLogger(Echo.class);
    private final static String message              = "Hello";

    public static final String  TIME_TO_CONNECT_MS   = "TimeToConnectMs";
    public static final String  TIME_TO_SEND_MS      = "TimeToSendMs";
    public static final String  TIME_TO_RECEIVE_MS   = "TimeToReceiveMs";
    public static final String  STATUS_KEY           = "Status";
    public static final String  STATUS_MESSAGE_KEY   = "StatusMsg";
    public static final String  STATUS_EXCEPTION     = "Exception";

    public static final String  SOCKET_PHASE_CONNECT = "connecting to";
    public static final String  SOCKET_PHASE_RECEIVE = "reading from";
    public static final String  SOCKET_PHASE_WRITE   = "writing to";

    public enum EchoStatus
    {
        OK, OPEN_FILE_LIMIT_ERROR, SOCKET_NO_IO, SOCKET_CONNECT_TIMEOUT, SEND_MESSAGE_TIMEOUT, RECEIVE_MESSAGE_TIMEOUT, MESSAGE_CORRUPT, SOCKET_IO_ERROR, HOST_IS_DOWN, NO_ROUTE_TO_HOST, UNKNOWN_HOST
    }

    /**
     * Tests a host for reachability.
     * 
     * @param hostName The host name of the echo server
     * @param portNumber The port number of the echo server
     * @param timeout Timeout in milliseconds
     * @return the result wrapped inside tungsten properties.
     */
    public static TungstenProperties isReachable(String hostName,
            int portNumber, int timeout)
    {
        TungstenProperties statusAndResult = new TungstenProperties();
        String statusMessage = null;
        Socket socket = null;
        InputStream socketInput = null;
        OutputStream socketOutput = null;
        String socketPhase = SOCKET_PHASE_CONNECT;
        EchoStatus timeoutPhase = EchoStatus.SOCKET_CONNECT_TIMEOUT;

        try
        {
            SocketAddress sockaddr = new InetSocketAddress(hostName, portNumber);
            socket = new Socket();
            socket.setSoTimeout(timeout);
            socket.setReuseAddress(true);

            long beforeConnect = System.currentTimeMillis();
            socket.connect(sockaddr, timeout);
            long timeToConnectMs = System.currentTimeMillis() - beforeConnect;
            statusAndResult.setLong(TIME_TO_CONNECT_MS, timeToConnectMs);

            socketInput = socket.getInputStream();
            socketOutput = socket.getOutputStream();

            if (socketInput == null || socketOutput == null)
            {
                statusMessage = String
                        .format("Socket connect error: InputStream=%s, OutputStream=%s after connect to %s:%s",
                                socketInput, socketOutput, hostName, portNumber);

                statusAndResult.setObject(STATUS_KEY, EchoStatus.SOCKET_NO_IO);
                statusAndResult.setString(STATUS_MESSAGE_KEY, statusMessage);
                logger.warn(formatExecStatus(statusAndResult));
                return logAndReturnProperties(statusAndResult);
            }

            int timeLeft = (int) (timeout - timeToConnectMs);
            if (timeLeft <= 0)
            {
                statusMessage = String
                        .format("Timeout while connecting: %d ms exceeds allowed timeout of %d ms.",
                                timeToConnectMs, timeout);

                statusAndResult.setObject(STATUS_KEY,
                        EchoStatus.SOCKET_CONNECT_TIMEOUT);
                statusAndResult.setString(STATUS_MESSAGE_KEY, statusMessage);
                logger.warn(formatExecStatus(statusAndResult));
                return logAndReturnProperties(statusAndResult);
            }

            socket.setSoTimeout(timeLeft);
            long beforeSend = System.currentTimeMillis();
            socketPhase = SOCKET_PHASE_WRITE;
            timeoutPhase = EchoStatus.SEND_MESSAGE_TIMEOUT;

            byte[] outBuff = message.getBytes();
            socketOutput.write(outBuff, 0, outBuff.length);
            long timeToSendMs = System.currentTimeMillis() - beforeSend;
            statusAndResult.setLong(TIME_TO_SEND_MS, timeToSendMs);

            timeLeft = (int) (timeLeft - timeToSendMs);
            if (timeLeft <= 0)
            {
                statusMessage = String
                        .format("Timeout while sending: %d ms exceeds allowed timeout of %d ms.",
                                timeToSendMs, timeLeft);

                statusAndResult.setObject(STATUS_KEY,
                        EchoStatus.SEND_MESSAGE_TIMEOUT);
                statusAndResult.setString(STATUS_MESSAGE_KEY, statusMessage);
                logger.warn(formatExecStatus(statusAndResult));
                return logAndReturnProperties(statusAndResult);
            }

            socket.setSoTimeout(timeLeft);
            long beforeReceive = System.currentTimeMillis();
            socketPhase = SOCKET_PHASE_RECEIVE;
            timeoutPhase = EchoStatus.RECEIVE_MESSAGE_TIMEOUT;

            byte[] inBuff = new byte[outBuff.length];
            int offset = 0;
            int length = 0;
            while (offset < inBuff.length)
            {
                length = socketInput.read(inBuff, offset, inBuff.length
                        - offset);
                offset += length;
            }
            String echoMessage = new String(inBuff);
            long timeToReceiveMs = System.currentTimeMillis() - beforeReceive;
            statusAndResult.setLong(TIME_TO_RECEIVE_MS, timeToReceiveMs);

            timeLeft = (int) (timeLeft - timeToReceiveMs);
            if (timeLeft <= 0)
            {
                statusMessage = String
                        .format("Timeout while reading: %d ms exceeds allowed timeout of %d ms.",
                                timeToReceiveMs, timeLeft);

                statusAndResult.setObject(STATUS_KEY,
                        EchoStatus.RECEIVE_MESSAGE_TIMEOUT);
                statusAndResult.setString(STATUS_MESSAGE_KEY, statusMessage);
                logger.warn(formatExecStatus(statusAndResult));
                return logAndReturnProperties(statusAndResult);
            }

            if (!message.equals(echoMessage))
            {
                statusMessage = String
                        .format("Corrupted message: expected '%s' with len=%d but got '%s' with len=%d.",
                                message, message.length(), echoMessage,
                                echoMessage.length());

                statusAndResult.setObject(STATUS_KEY,
                        EchoStatus.MESSAGE_CORRUPT);
                statusAndResult.setString(STATUS_MESSAGE_KEY, statusMessage);
                logger.warn(formatExecStatus(statusAndResult));
                return logAndReturnProperties(statusAndResult);
            }

            statusMessage = String.format("Ping to %s:%d succeeded.", hostName,
                    portNumber);
            statusAndResult.setString(STATUS_MESSAGE_KEY, statusMessage);
            statusAndResult.setObject(STATUS_KEY, EchoStatus.OK);
            return logAndReturnProperties(statusAndResult);
        }
        catch (SocketTimeoutException so)
        {
            statusMessage = String.format(
                    "Socket timeout while %s a socket %s:%d\nException='%s'",
                    socketPhase, hostName, portNumber, so);

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
                                hostName, socketPhase, hostName, portNumber,
                                ioe);
                statusAndResult.setObject(STATUS_KEY, EchoStatus.HOST_IS_DOWN);
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
                                hostName, socketPhase, hostName, portNumber,
                                ioe);

                statusAndResult.setObject(STATUS_KEY,
                        EchoStatus.NO_ROUTE_TO_HOST);
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
                                socketPhase, hostName, portNumber, ioe);
                statusAndResult.setObject(STATUS_KEY,
                        EchoStatus.OPEN_FILE_LIMIT_ERROR);
                statusAndResult.setString(STATUS_MESSAGE_KEY, statusMessage);
                statusAndResult.setObject(STATUS_EXCEPTION, ioe);
                logger.warn(formatExecStatus(statusAndResult));
                return logAndReturnProperties(statusAndResult);
            }

            if (ioe.toString().contains("java.net.UnknownHostException"))
            {
                statusMessage = String
                        .format("I/O exception while %s a socket to %s:%d\nException='%s'\n"
                                + "There may be an issue with your DNS for this host or your /etc/hosts entry is not correct.",
                                socketPhase, hostName, portNumber, ioe);
                statusAndResult.setObject(STATUS_KEY, EchoStatus.UNKNOWN_HOST);
                statusAndResult.setString(STATUS_MESSAGE_KEY, statusMessage);
                statusAndResult.setObject(STATUS_EXCEPTION, ioe);
                logger.warn(formatExecStatus(statusAndResult));
                return logAndReturnProperties(statusAndResult);
            }

            statusMessage = String
                    .format("I/O exception caught while %s a socket to %s:%d\nException='%s'",
                            socketPhase, hostName, portNumber, ioe);

            statusAndResult.setObject(STATUS_KEY, EchoStatus.SOCKET_IO_ERROR);
            statusAndResult.setString(STATUS_MESSAGE_KEY, statusMessage);
            statusAndResult.setObject(STATUS_EXCEPTION, ioe);
            logger.warn(formatExecStatus(statusAndResult));
            return logAndReturnProperties(statusAndResult);
        }
        finally
        {
            if (socketOutput != null)
            {
                try
                {
                    socketOutput.close();
                }
                catch (Exception ignored)
                {
                }
                finally
                {
                    socketOutput = null;
                }
            }

            if (socketInput != null)
            {
                try
                {
                    socketInput.close();
                }
                catch (Exception ignored)
                {
                }
                finally
                {
                    socketInput = null;
                }
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
                finally
                {
                    socket = null;
                }
            }
        }
    }

    /**
     * Formats a TungstenProperties for human-friendly output
     * 
     * @param props The tungsten properties to format
     * @return
     */
    private static String formatExecStatus(TungstenProperties props)
    {

        EchoStatus echoStatus = (EchoStatus) props.getObject(STATUS_KEY);
        String statusMessage = props.getString(STATUS_MESSAGE_KEY);

        return String.format("%s\n%s", echoStatus.toString(), statusMessage);
    }

    /**
     * Log the given TungstenProperties
     * 
     * @param props The tungsten properties to log
     * @return
     */

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