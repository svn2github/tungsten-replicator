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

package com.continuent.tungsten.common.sockets;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SocketChannel;

import javax.net.SocketFactory;
import javax.net.ssl.SSLSocketFactory;

import org.apache.log4j.Logger;

/**
 * Provides a wrapper for client connections via sockets. This class
 * encapsulates logic for timeouts, SSL vs. non-SSL operation, and closing the
 * connection.  This class assumes properties required for SSL operation 
 * have been previously set before SSL sockets are allocated. 
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class ClientSocketWrapper extends SocketWrapper
{
    private static Logger    logger = Logger.getLogger(ClientSocketWrapper.class);

    // Properties
    InetSocketAddress        address;
    private boolean          useSSL;
    private int              connectTimeout;
    private int              readTimeout;

    // Socket factory for new SSL connections.
    private SocketFactory    sslFactory;

    // Flag to signal service has been shut down.
    private volatile boolean done   = false;

    /** Creates a new wrapper for client connections. */
    public ClientSocketWrapper()
    {
        super(null);
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

    public long getConnectTimeout()
    {
        return connectTimeout;
    }

    /** Time in milliseconds before timeout when connecting to a server. */
    public void setConnectTimeout(int connectTimeout)
    {
        this.connectTimeout = connectTimeout;
    }

    public long getReadTimeout()
    {
        return readTimeout;
    }

    /**
     * Time in milliseconds before timeout when waiting for responses after
     * connection.
     */
    public void setReadTimeout(int readTimeout)
    {
        this.readTimeout = readTimeout;
    }

    /**
     * Connect to the server.
     */
    public Socket connect() throws IOException
    {
        // Create the socket.
        if (useSSL)
        {
            // Create an SSL socket.
            sslFactory = SSLSocketFactory.getDefault();
            socket = sslFactory.createSocket();
        }
        else
        {
            SocketChannel channel = SocketChannel.open();
            socket = channel.socket();
        }

        // Store the socket in the super class.
        setSocket(socket);

        // Try to connect using the connect timeout.
        try
        {
            socket.connect(address, connectTimeout);
        }
        catch (IOException e)
        {
            if (done)
            {
                throw new SocketTerminationException(
                        "Socket has been terminated", e);
            }
            else
            {
                throw e;
            }
        }

        // Disable Nagle's algorithm
        socket.setTcpNoDelay(true);
        // Enable TCP keepalive
        socket.setKeepAlive(true);
        // Set the socket timeout for reads.
        socket.setSoTimeout(readTimeout);

        return socket;
    }

    /** Returns the socket. */
    public Socket getSocket()
    {
        return this.socket;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.common.sockets.SocketWrapper#getInputStream()
     */
    @Override
    public InputStream getInputStream() throws IOException
    {
        return socket.getInputStream();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.common.sockets.SocketWrapper#getOutputStream()
     */
    @Override
    public OutputStream getOutputStream() throws IOException
    {
        return socket.getOutputStream();
    }

    /**
     * Close socket. This is synchronized to prevent accidental double calls.
     */
    public synchronized void close()
    {
        done = true;
        if (socket != null)
        {
            try
            {
                socket.close();
            }
            catch (IOException e)
            {
                logger.warn(e.getMessage());
            }
            finally
            {
                socket = null;
            }
        }
    }
}