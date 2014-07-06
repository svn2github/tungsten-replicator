/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2013-2014 Continuent Inc.
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

package com.continuent.tungsten.replicator.datasource;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.common.config.TungstenPropertiesIO;
import com.continuent.tungsten.common.file.FilePath;
import com.continuent.tungsten.common.file.HdfsFileIO;
import com.continuent.tungsten.replicator.ReplicatorException;

/**
 * Implements a data source that stores data in HDFS.
 */
public class HdfsDataSource extends AbstractDataSource
        implements
            UniversalDataSource
{
    private static Logger logger = Logger.getLogger(HdfsDataSource.class);

    // Properties.
    private String        hdfsUri;
    private String        hdfsConfigProperties;
    private String        directory;
    private URI           uri;

    // Catalog tables.
    FileCommitSeqno       commitSeqno;

    // File IO-related variables.
    FilePath              rootDir;
    FilePath              serviceDir;
    HdfsFileIO            hdfsFileIO;

    /** Create new instance. */
    public HdfsDataSource()
    {
    }

    public String getDirectory()
    {
        return directory;
    }

    public void setDirectory(String directory)
    {
        this.directory = directory;
    }

    public String getHdfsUri()
    {
        return hdfsUri;
    }

    public void setHdfsUri(String hdfsUri)
    {
        this.hdfsUri = hdfsUri;
    }

    public String getHdfsConfigProperties()
    {
        return hdfsConfigProperties;
    }

    public void setHdfsConfigProperties(String hdfsConfigProperties)
    {
        this.hdfsConfigProperties = hdfsConfigProperties;
    }

    /**
     * Instantiate and configure all data source tables.
     */
    @Override
    public void configure() throws ReplicatorException, InterruptedException
    {
        super.configure();

        // Configure file paths.
        rootDir = new FilePath(directory);
        serviceDir = new FilePath(rootDir, serviceName);

        // Ensure HDFS URI is ok.
        try
        {
            uri = new URI(hdfsUri);
        }
        catch (URISyntaxException e)
        {
            throw new ReplicatorException("Invalid HDFS URI: uri=" + uri
                    + " messsage=" + e.getMessage(), e);
        }

        // Load HDFS properties, if they exist.
        TungstenProperties hdfsProps;
        if (hdfsConfigProperties == null)
        {
            hdfsProps = new TungstenProperties();
        }
        else
        {
            File configPropFile = new File(hdfsConfigProperties);
            TungstenPropertiesIO propsIO = new TungstenPropertiesIO(
                    configPropFile);
            propsIO.setFormat(TungstenPropertiesIO.JAVA_PROPERTIES);
            hdfsProps = propsIO.read();
        }

        // Create HDFS FileIO instance.
        hdfsFileIO = new HdfsFileIO(uri, hdfsProps);

        // Configure tables.
        commitSeqno = new FileCommitSeqno(hdfsFileIO);
        commitSeqno.setServiceName(serviceName);
        commitSeqno.setChannels(channels);
        commitSeqno.setServiceDir(serviceDir);
    }

    /**
     * Prepare all data source tables for use.
     */
    @Override
    public void prepare() throws ReplicatorException, InterruptedException
    {
        // Make HDFS directory if it does not already exist.
        if (!hdfsFileIO.exists(serviceDir))
        {
            logger.info("Service directory does not exist, creating: "
                    + serviceDir.toString());
            hdfsFileIO.mkdirs(serviceDir);
        }

        // Ensure everything exists now.
        if (!hdfsFileIO.readable(serviceDir))
        {
            throw new ReplicatorException(
                    "Service directory does not exist or is not readable: "
                            + serviceDir.toString());
        }
        else if (!hdfsFileIO.writable(serviceDir))
        {
            throw new ReplicatorException("Service directory is not writable: "
                    + serviceDir.toString());
        }

        // Prepare all tables.
        commitSeqno.prepare();
    }

    /**
     * Release all data source tables.
     */
    @Override
    public void release() throws ReplicatorException, InterruptedException
    {
        // Release tables.
        if (commitSeqno != null)
        {
            commitSeqno.reduceTasks();
            commitSeqno.release();
            commitSeqno = null;
        }
    }

    /**
     * Ensure all tables are ready for use, creating them if necessary.
     */
    @Override
    public void initialize() throws ReplicatorException, InterruptedException
    {
        logger.info("Initializing data source files: service=" + serviceName
                + " directory=" + directory);
        commitSeqno.initialize();
    }

    @Override
    public void clear() throws ReplicatorException, InterruptedException
    {
        commitSeqno.clear();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.UniversalDataSource#getCommitSeqno()
     */
    @Override
    public CommitSeqno getCommitSeqno()
    {
        return commitSeqno;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.UniversalDataSource#getConnection()
     */
    public UniversalConnection getConnection() throws ReplicatorException
    {
        return new HdfsConnection(hdfsFileIO, csv);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.UniversalDataSource#releaseConnection(com.continuent.tungsten.replicator.datasource.UniversalConnection)
     */
    public void releaseConnection(UniversalConnection conn)
    {
        conn.close();
    }
}