/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2010-2011 Continuent Inc.
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

package com.continuent.tungsten.replicator.filter;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplOptionParams;
import com.continuent.tungsten.replicator.filter.Filter;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * Implements a filter to suppress events that originate on the local service
 * for remote slaves. This filter *must* run on slave pipelines for remote
 * slaves to prevent replication loops in bi-directional replication.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class BidiRemoteSlaveFilter implements Filter
{
    private static Logger logger                = Logger
                                                        .getLogger(BidiRemoteSlaveFilter.class);
    private boolean       remoteService;
    private String        thisServiceName;

    // Control parameters.
    private String        localServiceName;
    private boolean       allowBidiUnsafe       = false;
    private boolean       allowAnyRemoteService = false;

    /**
     * Sets the local service name. Events for this service are discarded.
     */
    public void setLocalServiceName(String localServiceName)
    {
        this.localServiceName = localServiceName;
    }

    /**
     * Sets whether we allow SQL that may be unsafe for bi-directional
     * replication.
     */
    public void setAllowBidiUnsafe(boolean allowBidiUnsafe)
    {
        this.allowBidiUnsafe = allowBidiUnsafe;
    }

    /**
     * Sets whether to allow all remote services or just this one.
     */
    public void setAllowAnyRemoteService(boolean allowAnyRemoteService)
    {
        this.allowAnyRemoteService = allowAnyRemoteService;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.filter.Filter#filter(com.continuent.tungsten.replicator.event.ReplDBMSEvent)
     */
    public ReplDBMSEvent filter(ReplDBMSEvent event)
            throws ReplicatorException, InterruptedException
    {
        // If we are not a remote service endpoint, the event is good.
        if (!remoteService)
        {
            if (logger.isDebugEnabled())
            {
                logger
                        .debug(String
                                .format(
                                        "RETURNING EVENT: service=%s, seqno=%d , service != REMOTE",
                                        thisServiceName, event.getSeqno()));
            }

            return event;
        }

        // Get key metadata.
        String originatingService = event.getDBMSEvent()
                .getMetadataOptionValue(ReplOptionParams.SERVICE);
        String bidiUnsafe = event.getDBMSEvent().getMetadataOptionValue(
                ReplOptionParams.BIDI_UNSAFE);

        // if (localServiceName.equals(originatingService))
        if (originatingService.equals(localServiceName))
        {
            // This one started here, so throw it away.
            if (logger.isDebugEnabled())
            {
                logger
                        .debug(String
                                .format(
                                        "DISCARDING EVENT: service=%s, localServiceName=%s EQUALS originatingService=%s, seqno=%d",
                                        thisServiceName, localServiceName,
                                        originatingService, event.getSeqno()));

            }
            return null;
        }
        else if (originatingService.equals(thisServiceName)
                || this.allowAnyRemoteService)
        {
            // Extra check is required to protect against bidi-unsafe values.
            if (!"true".equals(bidiUnsafe) || allowBidiUnsafe)
            {
                // All good, so we return the event unharmed.
                if (logger.isDebugEnabled())
                {
                    logger
                            .debug(String
                                    .format(
                                            "RETURNING EVENT: service=%s, originatingService=%s EQUALS thisServiceName=%s, seqno=%d",
                                            thisServiceName,
                                            originatingService,
                                            thisServiceName, event.getSeqno()));
                }
                return event;
            }
            else
            {
                // Suppress bi-di unsafe values.
                if (logger.isDebugEnabled())
                {
                    logger
                            .debug(String
                                    .format(
                                            "DISCARDING EVENT: Transaction contains bidi-unsafe commands, service=%s, seqno=%d",
                                            thisServiceName, event.getSeqno()));
                }
                return null;
            }
        }
        else
        {
            // This looks like another remote service name, not ours. This
            // case arises when there are more than 2 masters.
            if (logger.isDebugEnabled())
            {
                logger
                        .debug(String
                                .format(
                                        "DISCARDING EVENT: service=%s, originatingService=%s DOES NOT EQUAL thisServiceName=%s, seqno=%d",
                                        thisServiceName, originatingService,
                                        thisServiceName, event.getSeqno()));
            }
            return null;
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException
    {
        // See if we are remote and remember it.
        remoteService = context.isRemoteService();
        thisServiceName = context.getServiceName();

        // Error checks on the local service name, which must differ from the
        // replication service to which we belong.
        if (localServiceName == null)
            throw new ReplicatorException(
                    "Local service name (localServiceName) may not be null for this filter");
        else if (localServiceName.equals(context.getServiceName())
                && remoteService)
            throw new ReplicatorException(
                    "Local service name (localServiceName) must be different from the current service of this filter: localServiceName="
                            + localServiceName
                            + " current service="
                            + context.getServiceName());
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context) throws ReplicatorException
    {
        if (remoteService)
        {
            logger.info(String.format("Enabling remote slave filter: "
                    + "service=%s, localService=%s, allowBidiSafe=%s, "
                    + "allowAnyRemoteService=%s", thisServiceName,
                    localServiceName, allowBidiUnsafe, allowAnyRemoteService));
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void release(PluginContext context) throws ReplicatorException
    {
        // Nothing to do.
    }
}