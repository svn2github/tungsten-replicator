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
 * Initial developer(s): Robert Hodges
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.plugin;

import java.util.List;
import java.util.concurrent.Future;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.fsm.event.EventDispatcher;
import com.continuent.tungsten.replicator.conf.FailurePolicy;
import com.continuent.tungsten.replicator.conf.ReplicatorMonitor;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.pipeline.Stage;
import com.continuent.tungsten.replicator.service.PipelineService;
import com.continuent.tungsten.replicator.storage.Store;

/**
 * This class defines a context passed into replicator plugins that exposes
 * call-backs into the replicator itself to fetch configuration information and
 * invoke services.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public interface PluginContext
{
    /**
     * Returns the current replicator configuration properties.
     */
    public TungstenProperties getReplicatorProperties();

    /**
     * Returns current runtime options set when the replicator went online or
     * null if replicator has not gone online yet.
     */
    public TungstenProperties getOnlineOptions();

    /** Returns a JDBC URL suitable for login to local data source. */
    public String getJdbcUrl(String database);

    /** Returns a user for login to local data source. */
    public String getJdbcUser();

    /** Returns a password suitable for login to local data source. */
    public String getJdbcPassword();

    /** Schema name for storing replicator catalogs. */
    public String getReplicatorSchemaName();

    /** Returns the applier failure policy. */
    public abstract FailurePolicy getApplierFailurePolicy();

    /** Returns the extractorFailurePolicy value. */
    public abstract FailurePolicy getExtractorFailurePolicy();

    /** True if the replicator should stop on checksum failure. */
    public boolean isConsistencyFailureStop();

    /** Should consistency check be sensitive to column names? */
    public abstract boolean isConsistencyCheckColumnNames();

    /** Should consistency check be sensitive to column types? */
    public abstract boolean isConsistencyCheckColumnTypes();

    /**
     * Are checksums enabled? If false this overrides any local checksum
     * setting.
     */
    public abstract boolean isDoChecksum();

    /** Source ID for this replicator. */
    public abstract String getSourceId();

    /** Cluster name to which replicator belongs. */
    public abstract String getClusterName();

    /** Service name to which replication belongs. */
    public abstract String getServiceName();

    /** Returns the role name. */
    public abstract String getRoleName();

    /** Returns true if the replicator role is slave. */
    public abstract boolean isSlave();

    /** Returns true if the replicator role is master. */
    public abstract boolean isMaster();

    /** Returns true if replicator should go on-line automatically. */
    public abstract boolean isAutoEnable();

    /** Returns stages in pipeline order. */
    public abstract List<Stage> getStages();

    /** Returns a named storage component. */
    public abstract Store getStore(String name);

    /** Returns all stores. */
    public abstract List<Store> getStores();

    /** Returns a named pipeline service component. */
    public abstract PipelineService getService(String name);

    /** Returns all pipeline service components. */
    public abstract List<PipelineService> getServices();

    /** Returns the monitoring data object. */
    public ReplicatorMonitor getMonitor();

    /** Returns the event dispatcher for reporting interesting events. */
    public EventDispatcher getEventDispatcher();

    /** Returns the named extension or null if the extension does not exist. */
    public ReplicatorPlugin getExtension(String name);

    /** Returns the current list of extensions. */
    public List<String> getExtensionNames();

    /** Registers a JMX MBean from a lower-level component. */
    public void registerMBean(Object mbean, Class<?> mbeanClass, String name);

    /**
     * Returns true if the current replication pipeline belongs to a remote data
     * service. If so, applied updates must be logged to ensure correct
     * multi-master behavior.
     */
    public boolean isRemoteService();

    /**
     * Returns true if we want to log replicator updates. This is equivalent to
     * MySQL's log_slave_updates option.
     */
    public boolean logReplicatorUpdates();

    /** Returns true if the JDBC login for slave apply is a superuser. */
    public boolean isPrivilegedSlaveUpdate();

    /**
     * Return true if operating in native slave takeover mode.
     */
    public boolean nativeSlaveTakeover();

    /**
     * Returns the minimum safely committed sequence number from the end of the
     * pipeline. This sequence number can be used to free resources such as log
     * files used in upstream stages.
     */
    public long getCommittedSeqno();

    /**
     * Returns a future to allow clients to wait until the pipeline has safely
     * committed the requested seqno.
     * 
     * @throws InterruptedException
     */
    public Future<ReplDBMSHeader> waitForCommitted(long seqno)
            throws InterruptedException;

    /**
     * Registers the transaction source (if any) for the head of the current
     * pipeline. Should be used by head stage only to avoid confusion. For best
     * usability, head stages should set this as soon as it is known.
     * 
     * @param source Typical value is a URI of some kind
     */
    public void setPipelineSource(String source);

    /**
     * Returns the transaction source of the head stage in the pipeline.
     */
    public String getPipelineSource();

    /**
     * Returns the table type used for Tungsten catalog databases. This value is
     * meaningful only for MySQL databases and related data warehouse offspring
     * like InfiniDB and InfoBright.
     */
    public String getTungstenTableType();

    /**
     * Returns the number of channels in the tail stage of the pipeline or -1 if
     * pipeline is not active.
     */
    public int getChannels();
}