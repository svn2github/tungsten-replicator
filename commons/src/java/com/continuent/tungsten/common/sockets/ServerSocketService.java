/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2011-2012 Continuent Inc.
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

package com.continuent.tungsten.common.sockets;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.channels.ServerSocketChannel;

import javax.net.ServerSocketFactory;
import javax.net.ssl.SSLServerSocketFactory;

import org.apache.log4j.Logger;

/**
 * Provides a wrapper for managing server-side Socket connections. This class
 * encapsulates SSL vs. non-SSL operation, and closing the connection. The code
 * assumes properties required for SSL operation have been previously set before
 * SSL sockets are allocated.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class ServerSocketService
{
    private static Logger       logger        = Logger.getLogger(ServerSocketService.class);

    // Properties
    InetSocketAddress           address;
    private boolean             useSSL;
    private int                 acceptTimeout = 1000;

    // Socket factory for new SSL connections.
    private ServerSocketFactory sslServerFactory;

    // Currently open server socket.
    private ServerSocket        serverSocket  = null;

    // Flag to signal service has been shut down.
    private volatile boolean    done          = false;

    /** Creates a new wrapper for client connections. */
    public ServerSocketService()
    {
    }

    public InetSocketAddress getAddress()
    {
        return address;
    }

    /** Sets the address to which we should connect. */
    public void setAddress(InetSocketAddress address)
    {
        this.address = address;
    }

    public boolean isUseSSL()
    {
        return useSSL;
    }

    /** If set to true, use an SSL socket, otherwise use plain TCP/IP. */
    public void setUseSSL(boolean useSSL)
    {
        this.useSSL = useSSL;
    }

    // Accessors to server socket data.
    public int getLocalPort()
    {
        if (serverSocket != null)
            return serverSocket.getLocalPort();
        else
            return -1;
    }

    /**
     * Time in milliseconds before timeout to check for termination when
     * accepting connections. When using SSL this must be set to a low value as
     * SSL sockets are not interruptible during this operation.
     */
    public void setAcceptTimeout(int acceptTimeout)
    {
        this.acceptTimeout = acceptTimeout;
    }

    /**
     * Connect to the server socket.
     */
    public ServerSocket bind() throws IOException
    {
        // Create the serverSocket.
        if (useSSL)
        {
            // Open up SSL server socket.
            sslServerFactory = SSLServerSocketFactory.getDefault();
            serverSocket = sslServerFactory.createServerSocket();
        }
        else
        {
            ServerSocketChannel serverChannel = ServerSocketChannel.open();
            serverSocket = serverChannel.socket();
        }

        // Bind to the address.
        serverSocket.bind(address);
        serverSocket.setSoTimeout(acceptTimeout);
        serverSocket.setReuseAddress(true);

        return serverSocket;
    }

    /**
     * Accepts a connection the server socket.
     * 
     * @return A socket wrapper
     * @throws SocketTerminationException Thrown if the server socket has been
     *             terminated by a call to {@link #close()}
     * @throws IOException Thrown if a generic exception occurs during I/O
     */
    public SocketWrapper accept() throws IOException
    {
        for (;;)
        {
            try
            {
                Socket sock = serverSocket.accept();
                return new SocketWrapper(sock);
            }
            catch (SocketTimeoutException e)
            {
                // This is expected, since we time out on accept to
                // prevent hangs.
            }
            catch (IOException e)
            {
                if (done)
                {
                    throw new SocketTerminationException(
                            "Server socket has been terminated", e);
                }
                else
                {
                    throw e;
                }
            }
        }
    }

    /** Returns the server socket. */
    public ServerSocket getServerSocket()
    {
        return this.serverSocket;
    }

    /**
     * Close server socket. Client must call this from a separate thread to
     * break hangs on bind() and accept() calls. This is synchronized to prevent
     * accidental double calls.
     */
    public synchronized void close()
    {
        if (!done)
        {
            if (serverSocket != null && !serverSocket.isClosed())
            {
                try
                {
                    serverSocket.close();
                    done = true;
                }
                catch (IOException e)
                {
                    logger.warn(e.getMessage());
                }
            }
            else
            {
                logger.warn("Unable to close server socket (null or already closed)");
            }
        }
    }
}