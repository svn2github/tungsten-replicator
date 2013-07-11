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
import java.net.InetSocketAddress;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.config.cluster.ConfigurationException;

/**
 * Implements a simple client that connects to server and sends a string to the
 * server at regular intervals.
 */
public class EchoClient implements Runnable
{
    private static Logger       logger            = Logger.getLogger(EchoClient.class);

    // Client properties.
    private final String        host;
    private final int           port;
    private final boolean       useSSL;
    private final long          sleepMillis;

    // Operational variables.
    private ClientSocketWrapper socket;
    private volatile boolean    shutdownRequested = false;
    private boolean             isShutdown        = false;
    private Throwable           throwable;
    private Thread              clientThread;
    private volatile String     clientName;
    private int                 echoCount         = 0;

    /**
     * Create a new echo server instance.
     */
    public EchoClient(String host, int port, boolean useSSL, long sleepMillis)
    {
        this.host = host;
        this.port = port;
        this.useSSL = useSSL;
        this.sleepMillis = sleepMillis;
    }

    public Throwable getThrowable()
    {
        return throwable;
    }

    public int getEchoCount()
    {
        return echoCount;
    }

    public String getName()
    {
        return clientName;
    }

    /**
     * Starts the server.
     */
    public synchronized void start() throws IOException, ConfigurationException
    {
        // Configure and connect.
        logger.info("Connecting client to server: host=" + host + " port="
                + port + " useSSL=" + useSSL + " sleepMillis=" + sleepMillis);
        socket = new ClientSocketWrapper();
        socket.setAddress(new InetSocketAddress(host, port));
        socket.setUseSSL(useSSL);
        socket.connect();

        // Spawn ourselves in a separate server.
        clientThread = new Thread(this);
        clientName = clientThread.getName();
        clientThread.start();
        logger.info("Spawned client thread: " + clientName);
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
            logger.info("Client stopped by close on socket");
        }
        catch (InterruptedException e)
        {
            logger.info("Client stopped by interrupt on thread");
        }
        catch (Throwable t)
        {
            throwable = t;
            logger.info("Echo client failed: name=" + clientName
                    + " throwable=" + throwable.getMessage(), t);
        }
        finally
        {
            socket.close();
        }
    }

    /**
     * Implements basic server processing, which continues until a call to
     * shutdown or the thread is interrupted.
     */
    private void doRun() throws IOException, InterruptedException
    {
        SocketHelper helper = new SocketHelper();
        while (shutdownRequested == false)
        {
            String echoValue = helper.echo(socket.getSocket(), clientName);
            if (!clientName.equals(echoValue))
                throw new RuntimeException(
                        "Echo returned unexpected value: client=" + clientName
                                + " echoValue=" + echoValue);
            echoCount++;
            Thread.sleep(sleepMillis);
        }
    }

    /**
     * Shut down a running client nicely, returning true if the thread is
     * finished.
     */
    public synchronized boolean shutdown()
    {
        if (isShutdown)
            return !clientThread.isAlive();

        logger.info("Shutting down echo client: " + clientName + " echoCount="
                + echoCount);
        shutdownRequested = true;
        socket.close();
        clientThread.interrupt();
        try
        {
            clientThread.join(5000);
        }
        catch (InterruptedException e)
        {
            logger.warn("Unable to shut down echo client: " + clientName);
        }
        finally
        {
            isShutdown = true;
        }
        return !clientThread.isAlive();
    }
}