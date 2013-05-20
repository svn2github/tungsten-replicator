/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2010 Continuent Inc.
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
 * Initial developer(s): Teemu Ollakka
 * Contributor(s): Robert Hodges
 */

package com.continuent.tungsten.replicator.thl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.conf.ReplicatorConf;
import com.continuent.tungsten.replicator.plugin.PluginContext;
import com.continuent.tungsten.replicator.plugin.PluginLoader;
import com.continuent.tungsten.replicator.util.AtomicCounter;

/**
 * This class defines a Server
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public class Server implements Runnable
{
    private static Logger                         logger                = Logger.getLogger(Server.class);
    private PluginContext                         context;
    private Thread                                thd;
    private THL                                   thl;
    private String                                host;
    private int                                   port                  = 0;
    private ServerSocketChannel                   serverChannel;
    private ServerSocket                          socket;
    private LinkedList<ConnectorHandler>          clients               = new LinkedList<ConnectorHandler>();
    private LinkedBlockingQueue<ConnectorHandler> deadClients           = new LinkedBlockingQueue<ConnectorHandler>();
    private volatile boolean                      stopped               = false;
    private String                                storeName;

    /**
     * Determines how long a THL server will block in an accept() call before
     * yielding. If it doesn't yield periodically then, at least on some
     * kernels, it will be impossible to interrupt the thread calling except and
     * the replicator can hang on shutdown etc.
     */
    private int                                   serverAcceptTimeoutMs = 5000;

    /**
     * Creates a new <code>Server</code> object
     */
    public Server(PluginContext context, AtomicCounter sequencer, THL thl)
            throws ReplicatorException
    {
        this.context = context;
        this.thl = thl;
        this.storeName = thl.getName();

        String uriString = thl.getStorageListenerUri();
        URI uri;
        try
        {
            uri = new URI(uriString);

        }
        catch (URISyntaxException e)
        {
            throw new THLException("Malformed URI: " + uriString);
        }
        String protocol = uri.getScheme();
        if (protocol.equals(THL.URI_SCHEME) == false)
        {
            throw new THLException("Unsupported scheme " + protocol);
        }
        host = uri.getHost();
        if ((port = uri.getPort()) == -1)
        {
            port = 2112;
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Runnable#run()
     */
    public void run()
    {
        try
        {
            SocketChannel clientChannel;
            // Commenting the following line which leads accept() method not to
            // block and then CPU goes up to 100% (infinite loop)
            // serverChannel.configureBlocking(false);

            while (stopped == false)
            {

                /*
                 * This call will block until either a connection request is
                 * made or the timeout specified by serverAcceptTimeoutMs has
                 * elapsed.
                 */
                clientChannel = serverChannel.accept();

                /*
                 * We can end up with a null clientChannel if the accept() call
                 * timed out.
                 */
                if (clientChannel == null)
                {
                    continue;
                }

                ConnectorHandler handler = (ConnectorHandler) PluginLoader
                        .load(context.getReplicatorProperties().getString(
                                ReplicatorConf.THL_PROTOCOL,
                                ReplicatorConf.THL_PROTOCOL_DEFAULT, false)
                                + "Handler");
                handler.configure(context);
                handler.setChannel(clientChannel);
                handler.setServer(this);
                handler.setThl(thl);
                handler.prepare(context);

                clients.add(handler);
                handler.start();
                removeFinishedClients();

            }
        }
        catch (ClosedByInterruptException e)
        {
            if (stopped)
                logger.info("Server thread cancelled");
            else
                logger.info("THL server cancelled unexpectedly", e);
        }
        catch (IOException e)
        {
            logger.warn("THL server stopped by IOException; thread exiting", e);
        }
        catch (Throwable e)
        {
            logger.error("THL server terminated by unexpected error", e);
        }
        finally
        {
            // Close the connector handlers.
            logger.info("Closing connector handlers for THL Server: store="
                    + storeName);
            for (ConnectorHandler h : clients)
            {
                try
                {
                    h.stop();
                }
                catch (InterruptedException e)
                {
                    logger.warn("Connector handler close interrupted unexpectedly");
                }
                catch (Throwable t)
                {
                    logger.error("THL Server handler cleanup failed: store="
                            + storeName, t);
                }
            }

            // Remove finished clients.
            removeFinishedClients();
            if (clients.size() > 0)
            {
                logger.warn("One or more clients did not finish: "
                        + clients.size());
            }
            clients = null;

            // Close the socket.
            if (socket != null)
            {
                logger.info("Closing socket: store=" + storeName + " host="
                        + socket.getInetAddress() + " port="
                        + socket.getLocalPort());
                try
                {
                    socket.close();
                    socket = null;
                }
                catch (Throwable t)
                {
                    logger.error("THL Server socket cleanup failed: store="
                            + storeName, t);
                }
            }
            logger.info("THL thread done: store=" + storeName);
        }
    }

    /**
     * Marks a client for removal.
     */
    public void removeClient(ConnectorHandler client)
    {
        deadClients.offer(client);
    }

    /**
     * Clean up terminated clients marked for removal.
     */
    private void removeFinishedClients()
    {
        ConnectorHandler client = null;
        while ((client = deadClients.poll()) != null)
        {
            try
            {
                client.release(context);
            }
            catch (Exception e)
            {
                logger.warn("Failed to release connector handler", e);

            }
            clients.remove(client);
        }
    }

    /**
     * TODO: start definition.
     */
    public void start() throws IOException
    {
        serverAcceptTimeoutMs = context.getReplicatorProperties().getInt(
                ReplicatorConf.THL_SERVER_ACCEPT_TIMEOUT, null, true);

        logger.info("Opening THL server: store name=" + storeName + " host="
                + host + " port=" + port + " accept timeout="
                + serverAcceptTimeoutMs);

        serverChannel = ServerSocketChannel.open();
        socket = serverChannel.socket();
        socket.setSoTimeout(serverAcceptTimeoutMs);
        socket.bind(new InetSocketAddress(host, port));
        socket.setReuseAddress(true);
        logger.info("Opened socket: host=" + socket.getInetAddress() + " port="
                + socket.getLocalPort());

        thd = new Thread(this, "THL Server [" + storeName + ":" + host + ":"
                + port + "]");
        thd.start();
    }

    /**
     * TODO: stop definition.
     * 
     * @throws InterruptedException
     */
    public void stop() throws InterruptedException
    {
        // Signal that the server thread should stop.
        stopped = true;
        if (thd != null)
        {
            logger.info("Stopping server thread");
            thd.interrupt();
            if (serverChannel != null)
            {
                try
                {
                    serverChannel.close();
                }
                catch (IOException noted)
                {
                    logger.warn("Exception while closing channel:" + noted);
                }

            }
            logger.info("After interrupt...");
            do
            {
                if (!thd.isAlive())
                {
                    logger.info("Terminating join loop... thread is not alive");
                    break;
                }

                try
                {
                    Thread.sleep(2000);
                }
                catch (Exception ignored)
                {

                }

            }
            while (true);

            thd = null;

        }
    }

    /**
     * Returns the serverAcceptTimeoutMs value.
     * 
     * @return Returns the serverAcceptTimeoutMs.
     */
    public int getServerAcceptTimeoutMs()
    {
        return serverAcceptTimeoutMs;
    }

    /**
     * Sets the serverAcceptTimeoutMs value.
     * 
     * @param serverAcceptTimeoutMs The serverAcceptTimeoutMs to set.
     */
    public void setServerAcceptTimeoutMs(int serverAcceptTimeoutMs)
    {
        this.serverAcceptTimeoutMs = serverAcceptTimeoutMs;
    }

}
