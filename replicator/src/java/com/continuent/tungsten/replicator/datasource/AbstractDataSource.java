/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2014 Continuent Inc.
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

import java.util.TimeZone;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.csv.CsvSpecification;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.csv.CsvDataFormat;
import com.continuent.tungsten.replicator.csv.DefaultCsvDataFormat;

/**
 * Holds variables and implements methods that are common to all data sources.
 * Subclasses must call the {@link #configure()} method when configuring their
 * own state for this class to function correctly.
 */
public abstract class AbstractDataSource implements UniversalDataSource
{
    private static final Logger logger       = Logger.getLogger(AbstractDataSource.class);

    // Shared properties of all data sources.
    protected String            name;
    protected String            serviceName;
    protected int               channels     = 1;
    protected String            csvType;
    protected String            csvFormatter = DefaultCsvDataFormat.class
                                                     .getName();
    protected CsvSpecification  csv;

    // Shared variables.
    protected Class<?>          csvFormatterClass;

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public String getCsvType()
    {
        return csvType;
    }

    public void setCsvType(String csvType)
    {
        this.csvType = csvType;
    }

    public String getCsvFormatter()
    {
        return csvFormatter;
    }

    public void setCsvFormatter(String csvFormatterClass)
    {
        this.csvFormatter = csvFormatterClass;
    }

    public CsvSpecification getCsv()
    {
        return csv;
    }

    public void setCsv(CsvSpecification csv)
    {
        this.csv = csv;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.UniversalDataSource#setServiceName(java.lang.String)
     */
    public void setServiceName(String serviceName)
    {
        this.serviceName = serviceName;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.UniversalDataSource#getServiceName()
     */
    public String getServiceName()
    {
        return serviceName;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.UniversalDataSource#setChannels(int)
     */
    public void setChannels(int channels)
    {
        this.channels = channels;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.UniversalDataSource#getChannels()
     */
    public int getChannels()
    {
        return channels;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.CatalogEntity#configure()
     */
    public void configure() throws ReplicatorException, InterruptedException
    {
        // Check out the type of csv specification we have and proceed
        // accordingly.
        if (csvType == null)
        {
            logger.info("No cvsType provided; using default settings");
            csv = new CsvSpecification();
        }
        else if ("custom".equals(csvType))
        {
            logger.info("Using custom csvType defined by property settings");
            if (csv == null)
                throw new ReplicatorException(
                        "Custom CSV type settings missing for datasource");
        }
        else
        {
            logger.info("Using predefined csvType: name=" + csvType);
            csv = CsvSpecification.getSpecification(csvType);
            if (csv == null)
                throw new ReplicatorException("Unknown csvType: name="
                        + csvType);
        }

        // Load the CSV formatter class.
        try
        {
            // Load the CSV formatter class.
            logger.info("Checking CSV formatter class: " + csvFormatter);
            csvFormatterClass = Class.forName(csvFormatter);
        }
        catch (Exception e)
        {
            throw new ReplicatorException(
                    "Unable to load CSV formatter class: name=" + csvFormatter
                            + " message=" + e.getMessage(), e);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.UniversalDataSource#getCsvStringFormatter(java.util.TimeZone)
     */
    public CsvDataFormat getCsvStringFormatter(TimeZone tz)
            throws ReplicatorException
    {
        try
        {
            // Instantiate, configure, and return.
            CsvDataFormat formatter = (CsvDataFormat) csvFormatterClass
                    .newInstance();
            formatter.setTimeZone(tz);
            formatter.prepare();
            return formatter;
        }
        catch (Exception e)
        {
            throw new ReplicatorException(
                    "Unable to instantiate CSV formatter class: name="
                            + csvFormatter + " message=" + e.getMessage(), e);
        }
    }
}