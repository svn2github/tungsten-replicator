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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.config.cluster.ConfigurationException;

/**
 * Implements a simple echo server that echoes back bytes sent to it. The server
 * creates a new thread for every incoming request.
 */
public class EchoServer implements Runnable
{
    private static Logger                logger            = Logger.getLogger(EchoServer.class);
    private final String                 host;
    private final int                    port;
    private final boolean                useSSL;

    // Operational variables. These are volatile to permit concurrent access.
    private final ExecutorService        pool              = Executors
                                                                   .newFixedThreadPool(5);
    private volatile ServerSocketService socketService;
    private volatile boolean             shutdownRequested = false;
    private volatile Throwable           throwable;
    private volatile Thread              serverThread;

    /**
     * Create a new echo server instance.
     */
    public EchoServer(String host, int port, boolean useSSL)
    {
        this.host = host;
        this.port = port;
        this.useSSL = useSSL;
    }

    public Throwable getThrowable()
    {
        return throwable;
    }

    /**
     * Starts the server.
     */
    public void start() throws IOException, ConfigurationException
    {
        // Configure and connect.
        logger.info("Binding server: host=" + host + " port=" + port
                + " useSSL=" + useSSL);
        socketService = new ServerSocketService();
        socketService.setAddress(new InetSocketAddress(host, port));
        socketService.setUseSSL(useSSL);
        socketService.bind();

        // Spawn ourselves in a separate server.
        logger.info("Spawning server thread");
        serverThread = new Thread(this);
        serverThread.start();
    }

    /**
     * Loop through answering all incoming requests.
     */
    @Override
    public void run()
    {
        try
        {
            doRun();
        }
        catch (SocketTerminationException e)
        {
            logger.info("Server stopped by close on socket");
        }
        catch (InterruptedException e)
        {
            logger.info("Server stopped by interrupt on thread");
        }
        catch (Throwable t)
        {
            throwable = t;
            logger.info("Echo server failed: " + throwable.getMessage(), t);
        }
        finally
        {
            pool.shutdown();
            socketService.close();
        }
    }

    /**
     * Implements basic server processing, which continues until a call to
     * shutdown or the thread is interrupted.
     */
    private void doRun() throws IOException, InterruptedException
    {
        SocketWrapper client;
        while ((shutdownRequested == false)
                && (client = socketService.accept()) != null)
        {
            EchoSocketHandler handler = new EchoSocketHandler(this, client);
            pool.execute(handler);
        }
    }

    /** Shut down a running server nicely. */
    public void shutdown()
    {
        logger.info("Shutting down echo server");
        shutdownRequested = true;
        socketService.close();
        serverThread.interrupt();
        try
        {
            serverThread.join(5000);
        }
        catch (InterruptedException e)
        {
            logger.warn("Unable to shut down echo server");
        }
    }

    /** Shut down a running server after an error. */
    public void shutdownWithError(Throwable t)
    {
        this.throwable = t;
        shutdown();
    }
}

// Local class to implement a simple client handler.
class EchoSocketHandler implements Runnable
{
    private static Logger logger = Logger.getLogger(EchoSocketHandler.class);
    EchoServer            server;
    SocketWrapper         socketWrapper;

    EchoSocketHandler(EchoServer server, SocketWrapper socketWrapper)
    {
        this.server = server;
        this.socketWrapper = socketWrapper;
    }

    @Override
    public void run()
    {
        try
        {
            InputStream in = socketWrapper.getInputStream();
            OutputStream os = socketWrapper.getOutputStream();
            byte[] buffer = new byte[1024];
            int len;
            while ((len = in.read(buffer)) > 0 && !Thread.interrupted())
            {
                os.write(buffer, 0, len);
            }
        }
        catch (Throwable t)
        {
            logger.error("Socket handler failed: " + t.getMessage(), t);
            server.shutdownWithError(t);
        }
        finally
        {
            socketWrapper.close();
        }
    }
}