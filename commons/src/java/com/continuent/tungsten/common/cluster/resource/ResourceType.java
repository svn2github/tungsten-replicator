/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2009 Continuent Inc.
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
 * Initial developer(s): Ed Archibald
 * Contributor(s): 
 */

package com.continuent.tungsten.common.cluster.resource;

import java.io.Serializable;

public enum ResourceType implements Serializable
{
    ROOT, /* the root resource of any resource tree */
    EVENT, /* any application */
    CLUSTER, /* a cluster */
    MANAGER, /* a manager of the cluster */
    MEMBER, /* a cluster member */
    FOLDER, /* a general purpose folder */
    QUEUE, /* The resource represents an instance of a queue */
    CONFIGURATION, /*
                    * any type of configuration that can be represented as
                    * properties
                    */
    PROCESS, /* a JVM/MBean server */
    RESOURCE_MANAGER, /*
                       * a class that is exported as a JMX MBean for a specific
                       * component
                       */
    POLICY_MANAGER, /* represents a policy manager */
    OPERATION, /* an operation exported by a JMX MBean */
    DATASOURCE, /* a sql-router datasource */
    MONITOR, DATASERVER, /* a database server */
    HOST, /* a node in a cluster */
    SQLROUTER, /* a sql-router component */
    REPLICATOR, /* a replicator component */
    REPLICATION_SERVICE, /* a single service in a replicator */
    SERVICE_MANAGER, /* a tungsten-manager */
    SERVICE, DIRECTORY, /* a Directory instance */
    DIRECTORY_SESSION, UNDEFINED, EXTENSION, NONE, ANY
    /* any resource */
}
