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
 * Contributor(s): Linas Virbalas, Stephane Giron
 */

package com.continuent.tungsten.replicator.scripting;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.datasource.HdfsConnection;

/**
 * Provides a simple wrapper for HDFS connections that is suitable for exposure
 * in scripted environments. This is a delegate on the underlying HdfsConnection
 * class.
 */
public class HdfsWrapper
{
    private static Logger        logger = Logger.getLogger(HdfsWrapper.class);
    private final HdfsConnection connection;

    /** Creates a new instance. */
    public HdfsWrapper(HdfsConnection connection)
    {
        this.connection = connection;
    }

    /**
     * Delegate method on {@link HdfsConnection#put(String, String)}
     */
    public void put(String localFsPath, String hdfsPath)
            throws ReplicatorException
    {
        if (logger.isDebugEnabled())
            logger.debug(String.format(
                    "Copying from local file: %s to HDFS file: %s",
                    localFsPath, hdfsPath));
        connection.put(localFsPath, hdfsPath);
    }

    /**
     * Delegate method on {@link HdfsConnection#mkdir(String, boolean)}
     */
    public void mkdir(String path, boolean ignoreErrors)
            throws ReplicatorException
    {
        connection.mkdir(path, ignoreErrors);
    }

    /**
     * Delegate method on {@link HdfsConnection#rm(String, boolean, boolean)}
     */
    public void rm(String path, boolean recursive, boolean ignoreErrors)
            throws ReplicatorException
    {
        connection.rm(path, recursive, ignoreErrors);
    }

    /**
     * Releases the connection.
     */
    public void close()
    {
        connection.close();
    }
}