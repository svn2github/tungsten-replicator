/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2012 Continuent Inc.
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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.common.utils.ManifestParser;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.conf.ReplicatorConf;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSFilteredEvent;
import com.continuent.tungsten.replicator.event.ReplEvent;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * This class defines a Protocol
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public class Protocol
{
    private static Logger        logger                   = Logger.getLogger(Protocol.class);

    // Constants used for options and capabilities.
    private static String        SOURCE_ID                = "source_id";
    private static String        ROLE                     = "role";
    private static String        VERSION                  = "version";
    private static String        MIN_SEQNO                = "min_seqno";
    private static String        MAX_SEQNO                = "max_seqno";

    protected PluginContext      pluginContext            = null;
    protected SocketChannel      channel                  = null;

    // prefetchRange is a number of sequence number that are fetched
    // automatically (no need to send a message to the master for each sequence
    // number). Warning : a sequence number can be found several times in the
    // history table when the transaction was fragmented.
    private long                 prefetchRange            = Long.MAX_VALUE;
    private long                 prefetchIndex            = 0;
    private boolean              allPreviousFragmentsDone = true;

    protected int                resetPeriod;
    private int                  objectsSent              = 0;

    protected ObjectInputStream  ois                      = null;
    protected ObjectOutputStream oos                      = null;

    protected String             clientSourceId           = null;
    private long                 clientLastEpochNumber    = -1;
    private long                 clientLastSeqno          = -1;

    private int                  bufferSize;
    private ArrayList<ReplEvent> buffer                   = new ArrayList<ReplEvent>();
    private boolean              buffering                = false;

    /**
     * Creates a new <code>Protocol</code> object
     */
    public Protocol()
    {
    }

    /**
     * Creates a new <code>Protocol</code> object
     * 
     * @param channel
     * @throws IOException
     */
    public Protocol(PluginContext context, SocketChannel channel)
            throws IOException
    {
        this.pluginContext = context;
        this.channel = channel;

        oos = new ObjectOutputStream(new BufferedOutputStream(this.channel
                .socket().getOutputStream()));
        oos.flush();

        resetPeriod = 1;
    }

    public Protocol(PluginContext context, SocketChannel channel,
            int resetPeriod) throws IOException
    {
        this(context, channel);
        this.resetPeriod = resetPeriod;
        this.bufferSize = context.getReplicatorProperties().getInt(
                ReplicatorConf.THL_PROTOCOL_BUFFER_SIZE);
        buffering = bufferSize > 0;
        if (buffering && logger.isDebugEnabled())
            logger.debug("THL protocol buffering enabled: size=" + bufferSize);
    }

    /**
     * Returns the client source ID, which is set by a client protocol response
     * to a server.
     */
    public String getClientSourceId()
    {
        return clientSourceId;
    }

    /**
     * Returns the epoch number of last event received by client.
     */
    public long getClientLastEpochNumber()
    {
        return clientLastEpochNumber;
    }

    /**
     * Returns the log sequence number of last event received by client.
     */
    public long getClientLastSeqno()
    {
        return clientLastSeqno;
    }

    /**
     * TODO: readMessage definition.
     * 
     * @throws IOException
     * @throws ReplicatorException
     */
    protected ProtocolMessage readMessage() throws IOException,
            ReplicatorException
    {
        if (ois == null)
        {
            ois = new ObjectInputStream(new BufferedInputStream(this.channel
                    .socket().getInputStream()));
        }
        Object obj;
        try
        {
            obj = ois.readObject();
        }
        catch (ClassNotFoundException e)
        {
            throw new THLException(e.getMessage());
        }

        if (obj instanceof ProtocolMessage == false)
            throw new THLException("Invalid object in stream");
        return (ProtocolMessage) obj;
    }

    /**
     * TODO: writeMessage definition.
     * 
     * @param msg
     * @throws IOException
     * @throws RemoteProtocolException
     */
    protected void writeMessage(ProtocolMessage msg) throws IOException
    {
        oos.writeObject(msg);
        oos.flush();

        objectsSent++;
        if (objectsSent >= resetPeriod)
        {
            objectsSent = 0;
            oos.reset();
        }
    }

    /**
     * TODO: serverHandshake definition.
     * 
     * @param minSeqNo
     * @param maxSeqNo
     * @throws ReplicatorException
     * @throws IOException
     * @throws InterruptedException
     */
    public void serverHandshake(ProtocolHandshakeResponseValidator validator,
            long minSeqNo, long maxSeqNo) throws ReplicatorException,
            IOException, InterruptedException
    {
        ProtocolHandshake handshake = new ProtocolHandshake();
        handshake.setCapability(SOURCE_ID, pluginContext.getSourceId());
        handshake.setCapability(ROLE, pluginContext.getRoleName());
        handshake.setCapability(VERSION,
                ManifestParser.parseReleaseWithBuildNumber());
        handshake.setCapability(MIN_SEQNO, new Long(minSeqNo).toString());
        handshake.setCapability(MAX_SEQNO, new Long(maxSeqNo).toString());
        writeMessage(handshake);
        ProtocolMessage response = readMessage();
        if (response instanceof ProtocolHandshakeResponse)
        {
            ProtocolHandshakeResponse handshakeResponse = (ProtocolHandshakeResponse) response;
            this.clientSourceId = handshakeResponse.getSourceId();
            try
            {
                validator.validateResponse(handshakeResponse);
                writeMessage(new ProtocolOK(new SeqNoRange(minSeqNo, maxSeqNo)));
            }
            catch (THLException e)
            {
                writeMessage(new ProtocolNOK(
                        "Client response validation failed: " + e.getMessage()));
                throw e;
            }
        }
        else
        {
            writeMessage(new ProtocolNOK("Protocol error: message="
                    + response.getClass().getName()));
            throw new THLException("Protocol error: message="
                    + response.getClass().getName());
        }
    }

    /**
     * Define a client handshake event including attendant information. TODO:
     * clientHandshake definition.
     * 
     * @param lastEpochNumber Epoch number client has from last sequence number
     * @param lastSeqno Last sequence number client received
     * @param heartbeatMillis Number of milliseconds between heartbeat events
     * @return A sequence number range
     */
    public SeqNoRange clientHandshake(long lastEpochNumber, long lastSeqno,
            int heartbeatMillis, String lastEventId)
            throws ReplicatorException, IOException
    {
        ProtocolMessage handshake = readMessage();
        if (handshake instanceof ProtocolHandshake == false)
            throw new THLException("Invalid handshake");
        ProtocolHandshake protocolHandshake = (ProtocolHandshake) handshake;
        logger.info("Received master handshake: options="
                + new TungstenProperties(protocolHandshake.getCapabilities())
                        .toString());
        ProtocolHandshakeResponse response = new ProtocolHandshakeResponse(
                pluginContext.getSourceId(), lastEpochNumber, lastSeqno,
                heartbeatMillis);
        response.setOption(VERSION,
                ManifestParser.parseReleaseWithBuildNumber());
        if (lastEventId != null)
            response.setOption(ProtocolParams.INIT_EVENT_ID, lastEventId);
        writeMessage(response);

        ProtocolMessage okOrNok = readMessage();
        if (okOrNok instanceof ProtocolOK)
        {
            return (SeqNoRange) okOrNok.getPayload();
        }
        else if (okOrNok instanceof ProtocolNOK)
        {
            String msg = (String) okOrNok.getPayload();
            throw new THLException("Client handshake failure: " + msg);
        }
        else
        {
            throw new THLException("Unexpected server response: "
                    + okOrNok.getClass().getName());
        }
    }

    /**
     * TODO: requestReplicationDBMSEvent definition.
     * 
     * @param seqNo
     * @return sql event
     * @throws ReplicatorException
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    public ReplEvent requestReplEvent(long seqNo) throws ReplicatorException,
            IOException
    {
        ReplEvent ret = null;
        if (!buffer.isEmpty())
        {
            ret = buffer.remove(0);
        }
        else
        {
            if (prefetchIndex == 0 && allPreviousFragmentsDone)
            {
                writeMessage(new ProtocolReplEventRequest(seqNo, prefetchRange));
            }

            // Read the next message, skipping over any heartbeat events, which
            // serve to keep the connection open.
            ProtocolMessage msg = null;
            for (;;)
            {
                msg = readMessage();
                if (msg instanceof ProtocolHeartbeat)
                {
                    if (logger.isDebugEnabled())
                        logger.debug("Received protocol heartbeat");
                }
                else
                {
                    break;
                }
            }

            // Handling buffering on the client side
            if (msg.getPayload() instanceof ArrayList<?>)
            {
                // Receiving buffered events
                buffer = (ArrayList<ReplEvent>) msg.getPayload();
                if (!buffer.isEmpty())
                    ret = buffer.remove(0);
                else
                    logger.warn("Received an empty buffer");
            }
            else if (msg instanceof ProtocolNOK
                    && msg.getPayload() instanceof String)
            {
                String message = (String) msg.getPayload();
                throw new THLException(message);
            }
            else if (!(msg instanceof ProtocolReplEvent))
            {
                // Receiving an invalid message (neither a ProtocolReplEvent or
                // a list of ReplEvent)
                throw new THLException(
                        "Protocol error; unexpected protocol type: "
                                + msg.getClass().getName());
            }
            else
                ret = ((ProtocolReplEvent) msg).getReplEvent();
        }

        if (ret instanceof ReplDBMSEvent)
        {
            if (((ReplDBMSEvent) ret).getLastFrag())

            {
                allPreviousFragmentsDone = true;
                if (ret instanceof ReplDBMSFilteredEvent)
                {
                    ReplDBMSFilteredEvent event = (ReplDBMSFilteredEvent) ret;

                    if ((1 + prefetchIndex + event.getSeqnoEnd() - event
                            .getSeqno()) > prefetchRange)
                        prefetchIndex = 0;
                    else
                        prefetchIndex = (1 + prefetchIndex
                                + event.getSeqnoEnd() - event.getSeqno())
                                % prefetchRange;
                }
                else
                    prefetchIndex = (prefetchIndex + 1) % prefetchRange;
            }
            else
            {
                allPreviousFragmentsDone = false;
            }
        }
        else
        {
            allPreviousFragmentsDone = true;
            prefetchIndex = (prefetchIndex + 1) % prefetchRange;
        }

        return ret;
    }

    /**
     * TODO: waitReplicationDBMSEventRequest definition.
     * 
     * @return protocol event request
     * @throws ReplicatorException
     * @throws IOException
     */
    public ProtocolReplEventRequest waitReplEventRequest()
            throws ReplicatorException, IOException
    {
        ProtocolMessage msg = readMessage();
        if (msg instanceof ProtocolReplEventRequest == false)
            throw new THLException("Protocol error");
        return (ProtocolReplEventRequest) msg;
    }

    /**
     * TODO: sendReplicationDBMSEvent definition.
     * 
     * @param event
     * @param forceSend TODO
     * @throws IOException
     */
    public void sendReplEvent(ReplEvent event, boolean forceSend)
            throws IOException
    {
        if (buffering)
        {
            buffer.add(event);
            if (forceSend || buffer.size() >= bufferSize)
            {
                writeMessage(new ProtocolMessage(buffer));
                buffer.clear();
            }
        }
        else
        {
            writeMessage(new ProtocolReplEvent(event));
        }
    }

    /**
     * Send an error message back to client.
     */
    public void sendError(String message) throws IOException
    {
        if (buffering && buffer.size() > 0)
        {
            writeMessage(new ProtocolMessage(buffer));
            buffer.clear();
        }
        writeMessage(new ProtocolNOK(message));
    }

    /**
     * Send a heartbeat message to client.
     */
    public void sendHeartbeat() throws IOException
    {
        if (buffering && buffer.size() > 0)
        {
            writeMessage(new ProtocolMessage(buffer));
            buffer.clear();
        }
        writeMessage(new ProtocolHeartbeat());
    }
}
