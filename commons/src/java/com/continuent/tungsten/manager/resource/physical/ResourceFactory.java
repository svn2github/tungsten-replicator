/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2009 Continuent Inc.
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
 * Initial developer(s): Edward Archibald
 * Contributor(s): ______________________.
 */

package com.continuent.tungsten.manager.resource.physical;

import com.continuent.tungsten.common.cluster.resource.Queue;
import com.continuent.tungsten.common.cluster.resource.Resource;
import com.continuent.tungsten.common.cluster.resource.ResourceType;
import com.continuent.tungsten.common.directory.Directory;
import com.continuent.tungsten.common.directory.ResourceNode;
import com.continuent.tungsten.common.exception.ResourceException;
import com.continuent.tungsten.common.utils.ReflectUtils;
import com.continuent.tungsten.manager.resource.shared.Folder;
import com.continuent.tungsten.manager.resource.shared.ResourceConfiguration;

public class ResourceFactory
{
    /**
     * @param type
     * @param key
     * @param parent
     * @param directory
     * @param sessionID
     * @return the node created/added
     * @throws ResourceException
     */
    public static ResourceNode addInstance(ResourceType type, String key,
            ResourceNode parent, Directory directory, String sessionID)
            throws ResourceException
    {

        Resource newInstance = createInstance(sessionID, type, key, parent,
                directory);

        return parent.addChild(newInstance);
    }

    /**
     * Makes a copy of the source RouterResource and returns it with the new key
     * set.
     * 
     * @param source
     * @param destinationKey
     * @param destination
     * @param directory
     * @return a copy of the resource
     * @throws ResourceException
     */
    public static Resource copyInstance(ResourceNode source,
            String destinationKey, ResourceNode destination, Directory directory)
            throws ResourceException
    {

        Resource copy = (Resource) ReflectUtils.clone(source.getResource());

        if (copy.getType() != destination.getResource().getChildType())
        {
            throw new ResourceException(
                    String.format(
                            "cannot create a copy of type '%s' in destination for type '%s'",
                            copy.getType(), destination.getResource()
                                    .getChildType()));
        }

        copy.setName(destinationKey);
        return copy;
    }

    /**
     * Creates a new resource instance of the specified type
     * 
     * @param sessionID
     * @param type
     * @param key
     * @param parent
     * @param directory
     * @return the instance created
     * @throws ResourceException
     */
    public static Resource createInstance(String sessionID, ResourceType type,
            String key, ResourceNode parent, Directory directory)
            throws ResourceException
    {
        Resource newInstance = null;

        if (type == ResourceType.CLUSTER)
        {
            newInstance = createCluster(key, parent, directory);
        }
        else if (type == ResourceType.MANAGER)
        {
            newInstance = createClusterMember(key, parent, directory);
        }
        else if (type == ResourceType.PROCESS)
        {
            newInstance = createProcess(key, parent, directory);
        }
        else if (type == ResourceType.RESOURCE_MANAGER)
        {
            newInstance = createResourceManager(key, parent, directory);
        }
        else if (type == ResourceType.OPERATION)
        {
            newInstance = createOperation(key, parent, directory);
        }
        else if (type == ResourceType.FOLDER)
        {
            newInstance = createFolder(key, parent, directory);
        }
        else if (type == ResourceType.CONFIGURATION)
        {
            newInstance = createResourceConfiguration(key, parent, directory);
        }
        else
        {
            throw new ResourceException(
                    String.format(
                            "Unable to create new instance for resourceType=%s for parent of type=%s",
                            type, parent.getType()));
        }

        if (newInstance == null)
        {
            throw new ResourceException(
                    String.format(
                            "Unable to create new instance for resourceType=%s for parent of type=%s",
                            type, parent.getType()));
        }

        return newInstance;

    }

    private static Resource createCluster(String key, ResourceNode parent,
            Directory directory) throws ResourceException
    {

        if (parent.getResource().getType() != ResourceType.ROOT)
        {
            throw new ResourceException(
                    String.format(
                            "You cannot create a service in directory '%s' of type '%s'",
                            Directory.formatPath(Directory.getAbsolutePath(
                                    parent, null, true), true), parent
                                    .getResource().getType()));
        }

        Cluster cluster = new Cluster(key);

        return cluster;

    }

    public static <T> ResourceNode addQueue(String key, ResourceNode parent,
            T type, Directory directory) throws ResourceException
    {
        Queue<T> queue = new Queue<T>(key);

        return parent.addChild(queue);
    }

    private static Resource createProcess(String key, ResourceNode parent,
            Directory directory) throws ResourceException
    {
        if (parent.getResource().getType() != ResourceType.MANAGER)
        {
            throw new ResourceException(
                    String.format(
                            "You cannot create a service in directory '%s' of type '%s'",
                            Directory.formatPath(Directory.getAbsolutePath(
                                    parent, null, true), true), parent
                                    .getResource().getType()));
        }

        Process process = new Process(key);

        return process;

    }

    private static Resource createResourceManager(String key,
            ResourceNode parent, Directory directory) throws ResourceException
    {

        if (parent.getResource().getType() != ResourceType.PROCESS)
        {
            throw new ResourceException(
                    String.format(
                            "You cannot create a resource manager in directory '%s' of type '%s'",
                            Directory.formatPath(Directory.getAbsolutePath(
                                    parent, null, true), true), parent
                                    .getResource().getType()));
        }

        ResourceManager resourceManager = new ResourceManager(key);

        return resourceManager;

    }

    private static Resource createClusterMember(String key,
            ResourceNode parent, Directory directory) throws ResourceException
    {

        if (parent.getResource().getType() != ResourceType.CLUSTER)
        {
            throw new ResourceException(
                    String.format(
                            "You cannot create a cluster member in directory '%s' of type '%s'",
                            Directory.formatPath(Directory.getAbsolutePath(
                                    parent, null, true), true), parent
                                    .getResource().getType()));
        }

        ClusterManager clusterManager = new ClusterManager(key);

        return clusterManager;

    }

    private static Resource createFolder(String key, ResourceNode parent,
            Directory directory) throws ResourceException
    {
        Folder folder = new Folder(key);

        return folder;

    }

    private static Resource createResourceConfiguration(String key,
            ResourceNode parent, Directory directory) throws ResourceException
    {
        ResourceConfiguration config = new ResourceConfiguration(key);

        return config;

    }

    private static Resource createOperation(String key, ResourceNode parent,
            Directory directory) throws ResourceException
    {

        if (parent.getResource().getType() != ResourceType.RESOURCE_MANAGER)
        {
            throw new ResourceException(
                    String.format(
                            "You cannot create an operation in directory '%s' of type '%s'",
                            Directory.formatPath(Directory.getAbsolutePath(
                                    parent, null, true), true), parent
                                    .getResource().getType()));
        }

        Operation operation = new Operation(key);

        return operation;

    }

}
