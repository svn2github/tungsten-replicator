/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2013 Continuent Inc.
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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.sockets.ServerSocketService;
import com.continuent.tungsten.common.sockets.SocketTerminationException;
import com.continuent.tungsten.common.sockets.SocketWrapper;
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
    private static Logger                         logger      = Logger.getLogger(Server.class);
    private PluginContext                         context;
    private Thread                                thd;
    private THL                                   thl;
    private String                                host;
    private int                                   port        = 0;
    private boolean                               useSSL;
    private ServerSocketService                   socketService;
    private LinkedList<ConnectorHandler>          clients     = new LinkedList<ConnectorHandler>();
    private LinkedBlockingQueue<ConnectorHandler> deadClients = new LinkedBlockingQueue<ConnectorHandler>();
    private volatile boolean                      stopped     = false;
    private String                                storeName;

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
        if (THL.PLAINTEXT_URI_SCHEME.equals(protocol))
        {
            this.useSSL = false;
        }
        else if (THL.SSL_URI_SCHEME.equals(protocol))
        {
            this.useSSL = true;
        }
        else
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
            SocketWrapper socket;
            while ((stopped == false)
                    && (socket = this.socketService.accept()) != null)
            {
                ConnectorHandler handler = (ConnectorHandler) PluginLoader
                        .load(context.getReplicatorProperties().getString(
                                ReplicatorConf.THL_PROTOCOL,
                                ReplicatorConf.THL_PROTOCOL_DEFAULT, false)
                                + "Handler");
                handler.configure(context);
                handler.setSocket(socket);
                handler.setServer(this);
                handler.setThl(thl);
                handler.prepare(context);

                clients.add(handler);
                handler.start();
                removeFinishedClients();
            }
        }
        catch (SocketTerminationException e)
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
            if (socketService != null)
            {
                logger.info("Closing socket: store=" + storeName + " host="
                        + socketService.getAddress() + " port="
                        + socketService.getLocalPort());
                try
                {
                    socketService.close();
                    socketService = null;
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
        logger.info("Opening THL server: store name=" + storeName + " host="
                + host + " port=" + port);

        socketService = new ServerSocketService();
        socketService.setAddress(new InetSocketAddress(host, port));
        socketService.setUseSSL(useSSL);
        socketService.bind();
        logger.info("Opened socket: host=" + socketService.getAddress()
                + " port=" + socketService.getLocalPort() + " useSSL=" + useSSL);

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
            try
            {
                logger.info("Stopping server thread");
                socketService.close();
                thd.interrupt();
                thd.join();
                thd = null;
            }
            catch (InterruptedException e)
            {
                logger.info("THL stop operation interrupted: " + e);
                throw e;
            }
        }
    }

}
