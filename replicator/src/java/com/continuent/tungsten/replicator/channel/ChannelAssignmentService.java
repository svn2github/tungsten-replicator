/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2011-13 Continuent Inc.
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

package com.continuent.tungsten.replicator.channel;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.DatabaseFactory;
import com.continuent.tungsten.replicator.plugin.PluginContext;
import com.continuent.tungsten.replicator.service.PipelineService;

/**
 * Provides a service interface to the shard-to-channel assignment table. This
 * service only works for relational databases and deactivates automatically if
 * the URL is not set. This is necessary to permit proper operation when
 * applying against NoSQL DBMS like MongoDB.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class ChannelAssignmentService implements PipelineService
{
    // Parameters.
    private static Logger        logger                    = Logger.getLogger(ChannelAssignmentService.class);
    private String               name;
    private String               user;
    private String               url;
    private String               password;
    private int                  channels;

    // Internal values.
    private boolean              active                    = false;
    private Database             conn;
    private ShardChannelTable    channelTable;
    private Map<String, Integer> assignments               = new HashMap<String, Integer>();
    private int                  maxChannel                = -1;
    private int                  nextChannel               = 0;
    private int                  accessFailures;
    private long                 reconnectTimeoutInSeconds = 60;
    private long                 connectionLastUsedTime;

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public void setUser(String user)
    {
        this.user = user;
    }

    public void setUrl(String url)
    {
        this.url = url;
    }

    public void setPassword(String password)
    {
        this.password = password;
    }

    public void setChannels(int channels)
    {
        this.channels = channels;
    }

    /**
     * Sets the reconnectTimeoutInSeconds in seconds. Connection will get
     * renewed if it has not been used for more than this time.
     * 
     * @param reconnectTimeoutInSeconds The reconnectTimeoutInSeconds to set in
     *            seconds.
     */
    public void setReconnectTimeoutInSeconds(long reconnectTimeoutInSeconds)
    {
        this.reconnectTimeoutInSeconds = reconnectTimeoutInSeconds;
    }

    /** Returns true if the channel assignment service is active. */
    public boolean isActive()
    {
        return active;
    }

    /**
     * Ensures that the service is active.
     * 
     * @throws ReplicatorException Thrown if service is inactive
     */
    private void assertActive() throws ReplicatorException
    {
        if (!active)
        {
            throw new ReplicatorException(
                    "Channel assignment service is not enabled");
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        // If the URL is missing, the service is not active. This is expected if
        // the target is non-relational.
        if (url == null || url.trim().length() == 0)
        {
            logger.info("Channel-assignment service URL is null; service is disabled");
            return;
        }
        else
        {
            active = true;
        }

        // Use default user/password if not supplied.
        if (user == null)
            user = context.getJdbcUser();
        if (password == null)
            password = context.getJdbcPassword();

        // Create the database connection.
        try
        {
            conn = DatabaseFactory.createDatabase(url, user, password,
                    context.isPrivilegedSlave());
            if (reconnectTimeoutInSeconds > 0)
            {
                logger.info("ChannelAssignmentService will use a "
                        + reconnectTimeoutInSeconds + "s timeout.");
                connectionLastUsedTime = System.currentTimeMillis();
            }
            // Need to suppress logging if possible. If we are running on a
            // master we also don't want to run without a privileged account to
            // avoid corrupting downstream replicators. However that check needs
            // to happen when we actually log data.
            conn.connect();
            if (context.isSlave() && context.isPrivilegedSlave())
            {
                if (conn.supportsControlSessionLevelLogging())
                    conn.controlSessionLevelLogging(true);
            }
        }
        catch (SQLException e)
        {
            throw new ReplicatorException("Unable to connect to database: "
                    + e.getMessage(), e);
        }

        String metadataSchema = context.getReplicatorSchemaName();

        // Create shard-channel table if it does not exist.
        channelTable = new ShardChannelTable(metadataSchema,
                context.getTungstenTableType());
        try
        {
            // HACK: Create schema. This table should be created by
            // the catalog manager.
            if (conn.supportsUseDefaultSchema() && metadataSchema != null)
            {
                if (conn.supportsCreateDropSchema())
                    conn.createSchema(metadataSchema);
                conn.useDefaultSchema(metadataSchema);
            }
            channelTable.initializeShardTable(conn, context.getChannels());
        }
        catch (SQLException e)
        {
            throw new ReplicatorException(
                    "Unable to initialize shard-channel table", e);
        }

        // Load channel assignments.
        loadChannelAssignments();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    @Override
    public void release(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        if (conn != null)
        {
            conn.close();
            conn = null;
        }
    }

    /**
     * Return a list of current channel assignments.
     */
    public synchronized List<Map<String, String>> listChannelAssignments()
            throws ReplicatorException
    {
        assertActive();
        List<Map<String, String>> channels = null;
        try
        {
            channels = channelTable.list(getConnection());
        }
        catch (SQLException e)
        {
            accessFailures++;
            if (logger.isDebugEnabled())
                logger.debug("Channel table access failed", e);
            channels = new ArrayList<Map<String, String>>();
        }
        return channels;
    }

    /**
     * Inserts a shard/channel assignment.
     * 
     * @param shardId Shard name
     * @param channel Channel number
     * @throws ReplicatorException Thrown if there is an error accessing
     *             database
     */
    public synchronized void insertChannelAssignment(String shardId, int channel)
            throws ReplicatorException
    {
        assertActive();
        try
        {
            channelTable.insert(getConnection(), shardId, channel);
            if (channel > maxChannel)
                maxChannel = channel;
            assignments.put(shardId, channel);
        }
        catch (SQLException e)
        {
            throw new ReplicatorException(
                    "Unable to access channel assignment table; ensure it is defined",
                    e);
        }
    }

    /**
     * Return the internal connection, renewing it if needed.
     * 
     * @return Dataabase connection used by the ChannelAssignmentService
     * @throws SQLException
     */
    private Database getConnection() throws SQLException
    {
        reconnectIfNeeded();
        return conn;
    }

    private void reconnectIfNeeded() throws SQLException
    {
        long currentTime = System.currentTimeMillis();
        if (reconnectTimeoutInSeconds > 0
                && currentTime - connectionLastUsedTime > reconnectTimeoutInSeconds * 1000)
        {
            if (logger.isDebugEnabled())
                logger.debug("Renewing connection (last active "
                        + (currentTime - connectionLastUsedTime) / 1000
                        + "s ago)");
            // Time to reconnect now
            conn.close();
            conn.connect();
        }
        else if (logger.isDebugEnabled())
            logger.debug("Not renewing connection (last active "
                    + (currentTime - connectionLastUsedTime) / 1000 + "s ago)");
        connectionLastUsedTime = currentTime;
    }

    /**
     * Looks up a channel assignment for a shard. This creates a new assignment
     * if required.
     * 
     * @param shardId Shard name
     * @return Integer channel number for shard
     * @throws ReplicatorException Thrown if there is an error accessing
     *             database
     */
    public synchronized Integer getChannelAssignment(String shardId)
            throws ReplicatorException
    {
        // See if we have a channel.
        assertActive();
        Integer channel = assignments.get(shardId);

        // If not we need to create a brand new assignment.
        if (channel == null)
        {
            // Roll over partition number if necessary.
            if (nextChannel >= channels)
            {
                nextChannel = 0;
            }
            channel = nextChannel++;

            // Assign the new partition in the channel assignment
            // table.
            insertChannelAssignment(shardId, channel);
        }

        // Return the channel.
        return channel;
    }

    // Load current channel assignments from the database.
    private synchronized void loadChannelAssignments()
            throws ReplicatorException
    {
        try
        {
            List<Map<String, String>> rows = channelTable.list(getConnection());
            for (Map<String, String> assignment : rows)
            {
                // Populate the table.
                String shardId = assignment.get(ShardChannelTable.SHARD_ID_COL);
                Integer channel = Integer.parseInt(assignment
                        .get(ShardChannelTable.CHANNEL_COL));
                assignments.put(shardId, channel);

                // Track the maximum channel.
                if (channel > maxChannel)
                    maxChannel = channel;
            }
        }
        catch (SQLException e)
        {
            throw new ReplicatorException(
                    "Unable to access shard assignment table; ensure it is defined",
                    e);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.service.PipelineService#status()
     */
    public TungstenProperties status()
    {
        TungstenProperties props = new TungstenProperties();
        props.setString("name", name);
        props.setLong("totalAssignments", assignments.size());
        props.setLong("maxChannel", maxChannel);
        props.setLong("accessFailures", accessFailures);
        props.setBoolean("active", active);
        return props;
    }
}