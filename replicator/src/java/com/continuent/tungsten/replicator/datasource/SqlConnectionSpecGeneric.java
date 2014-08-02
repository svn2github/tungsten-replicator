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
 * Contributor(s): Linas Virbalas
 */

package com.continuent.tungsten.replicator.datasource;

/**
 * Implements properties common to all SQL connections including boilerplate
 * accessor code required by Java.
 */
public class SqlConnectionSpecGeneric implements SqlConnectionSpec
{
    // Properties.
    protected String  vendor;
    protected String  user;
    protected String  password;
    protected String  host;
    protected int     port;
    protected String  tableType;
    protected boolean privilegedSlaveUpdate = true;
    protected boolean logSlaveUpdates       = false;
    protected boolean sslEnabled;
    protected String  schema;

    // Url may be specified or generated.
    protected String  url;

    /** Generic constructor to make this fit bean semantics. */
    public SqlConnectionSpecGeneric()
    {
    }

    /**
     * Returns the DBMS login.
     */
    public String getUser()
    {
        return user;
    }

    public void setUser(String user)
    {
        this.user = user;
    }

    /**
     * Returns the DBMS password.
     */
    public String getPassword()
    {
        return password;
    }

    public void setPassword(String password)
    {
        this.password = password;
    }

    /**
     * Returns the DBMS host.
     */
    public String getHost()
    {
        return host;
    }

    public void setHost(String host)
    {
        this.host = host;
    }

    /**
     * Returns the DBMS port.
     */
    public int getPort()
    {
        return port;
    }

    public void setPort(int port)
    {
        this.port = port;
    }

    /**
     * Returns the DBMS table type.
     */
    public String getTableType()
    {
        return tableType;
    }

    public void setTableType(String tableType)
    {
        this.tableType = tableType;
    }
    
    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.SqlConnectionSpec#getVendor()
     */
    public String getVendor()
    {
        return vendor;
    }

    public void setVendor(String vendor)
    {
        this.vendor = vendor;
    }

    /**
     * Returns whether privileged slave updates are enabled.
     */
    public boolean isPrivilegedSlaveUpdate()
    {
        return privilegedSlaveUpdate;
    }

    public void setPrivilegedSlaveUpdate(boolean privilegedSlaveUpdate)
    {
        this.privilegedSlaveUpdate = privilegedSlaveUpdate;
    }

    public boolean isLogSlaveUpdates()
    {
        return logSlaveUpdates;
    }

    public void setLogSlaveUpdates(boolean logSlaveUpdates)
    {
        this.logSlaveUpdates = logSlaveUpdates;
    }

    /**
     * Returns the DBMS schema for catalog tables.
     */
    public String getSchema()
    {
        return schema;
    }

    public void setSchema(String schema)
    {
        this.schema = schema;
    }

    /**
     * If true, connection must be SSL-enabled.
     */
    public boolean isSslEnabled()
    {
        return sslEnabled;
    }

    public void setSslEnabled(boolean sslEnabled)
    {
        this.sslEnabled = sslEnabled;
    }

    /** Sets an optional URL. Subclasses may compute this if it is absent. */
    public void setUrl(String url)
    {
        this.url = url;
    }

    public String getUrl()
    {
        return url;
    }

    /**
     * Must be implemented by subclasses.
     */
    public String createUrl(boolean createDB)
    {
        return getUrl();
    }
}