/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2011-2014 Continuent Inc.
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

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;

/**
 * Implements a dummy data source type for testing.
 */
public class SampleDataSource extends AbstractDataSource
        implements
            UniversalDataSource
{
    private static Logger logger = Logger.getLogger(SampleDataSource.class);

    // Properties.
    private String        myParameter;

    /** Create new instance. */
    public SampleDataSource()
    {
    }

    public String getMyParameter()
    {
        return myParameter;
    }

    public void setMyParameter(String myParameter)
    {
        this.myParameter = myParameter;
    }

    // CATALOG API

    /**
     * Instantiate and configure all data source tables.
     */
    @Override
    public void configure() throws ReplicatorException, InterruptedException
    {
        logger.info("Configuring data source: service=" + serviceName);
    }

    /**
     * Prepare all data source tables for use.
     */
    @Override
    public void prepare() throws ReplicatorException, InterruptedException
    {
        logger.info("Preparing data source: service=" + serviceName);
    }

    /**
     * Release all data source tables.
     */
    @Override
    public void release() throws ReplicatorException, InterruptedException
    {
        logger.info("Releasing data source: service=" + serviceName);
    }

    /**
     * Ensure all tables are ready for use, creating them if necessary.
     */
    @Override
    public void initialize() throws ReplicatorException, InterruptedException
    {
        logger.info("Initializing data source tables: service=" + serviceName);
    }

    @Override
    public void clear() throws ReplicatorException, InterruptedException
    {
        logger.info("Clearing data source tables: service=" + serviceName);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.UniversalDataSource#getCommitSeqno()
     */
    @Override
    public CommitSeqno getCommitSeqno()
    {
        return null;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.UniversalDataSource#getConnection()
     */
    public UniversalConnection getConnection() throws ReplicatorException
    {
        // Not implemented for now.
        return null;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.UniversalDataSource#releaseConnection(com.continuent.tungsten.replicator.datasource.UniversalConnection)
     */
    public void releaseConnection(UniversalConnection conn)
    {
        // Not implemented for now.
    }
}