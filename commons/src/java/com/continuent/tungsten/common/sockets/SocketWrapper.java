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
import java.net.InetAddress;
import java.net.Socket;

import org.apache.log4j.Logger;

/**
 * Implements methods common to both client and server sockets. It provides
 * common methods for obtaining input and output streams on both socket types as
 * well as closing the socket. This class works around the fact that the Java
 * NIO Channel provides an incomplete abstraction for networking that does not
 * support SSL operation.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class SocketWrapper
{
    private static Logger logger = Logger.getLogger(SocketWrapper.class);

    protected Socket      socket = null;

    /**
     * Creates a new socket wrapper.
     * 
     * @param socket The socket to wrap or null if the socket has not yet been
     *            connected
     */
    SocketWrapper(Socket socket)
    {
        this.socket = socket;
    }

    /**
     * Sets the socket. This is used by clients sockets, which do not know the
     * socket type until they connect.
     */
    public void setSocket(Socket socket)
    {
        this.socket = socket;
    }

    /** Returns the socket. */
    public Socket getSocket()
    {
        return this.socket;
    }

    /**
     * Returns an input stream that can read data from the socket.
     */
    public InputStream getInputStream() throws IOException
    {
        return socket.getInputStream();
    }

    /**
     * Returns an output stream that can write data to the socket.
     */
    public OutputStream getOutputStream() throws IOException
    {
        return socket.getOutputStream();
    }

    /**
     * Close socket. This is synchronized to prevent accidental double calls.
     */
    public synchronized void close()
    {
        if (socket != null && !socket.isClosed())
        {
            try
            {
                socket.close();
            }
            catch (IOException e)
            {
                logger.warn(e.getMessage());
            }
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#toString()
     */
    public String toString()
    {
        StringBuffer sb = new StringBuffer();
        sb.append(this.getClass().getSimpleName());
        if (socket == null)
        {
            sb.append(" [unbound]");
        }
        else
        {
            sb.append(" impl=").append(socket.getClass().getSimpleName());
            sb.append(" closed=").append(socket.isClosed());
            InetAddress address = socket.getInetAddress();
            sb.append(" local port=").append(socket.getLocalPort());
            if (address != null)
            {
                sb.append(" remote address=").append(address.getHostAddress());
            }
        }
        return sb.toString();
    }
}