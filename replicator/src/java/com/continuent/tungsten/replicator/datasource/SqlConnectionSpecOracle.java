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

/**
 * Denotes a class that can generate URLs for a specific DBMS type.
 */
public class SqlConnectionSpecOracle extends SqlConnectionSpecGeneric
{
    // Extra properties supported by MySQL connections.
    protected String jdbcHeader;
    protected String urlOptions;
    private String   serviceName = null;
    private String   sid         = null;

    public String getServiceName()
    {
        return serviceName;
    }

    /**
     * Instantiate URLa specification for MySQL with InnoDB as default table
     * type.
     */
    public SqlConnectionSpecOracle()
    {
        this.tableType = "CDC";
    }

    public void setServiceName(String serviceName)
    {
        this.serviceName = serviceName;
    }

    /**
     * Returns the JDBC URL header, e.g., a prefix like "jdbc:mysql:thin://" or
     * null if we are to use a default.
     */
    public String getJdbcHeader()
    {
        return jdbcHeader;
    }

    public void setJdbcHeader(String jdbcHeader)
    {
        this.jdbcHeader = jdbcHeader;
    }

    /**
     * Returns extra URL options added at the discretion of clients.
     */
    public String getUrlOptions()
    {
        return urlOptions;
    }

    public void setUrlOptions(String urlOptions)
    {
        this.urlOptions = urlOptions;
    }

    public String getSid()
    {
        return sid;
    }

    public void setSid(String sid)
    {
        this.sid = sid;
    }

    /**
     * Indicate that MySQL can create database from the URL.
     */
    @Override
    public boolean supportsCreateDB()
    {
        return false;
    }

    /**
     * Generates a MySQL URL with or without the createDB=true option. This
     * option should *only* be used the first time we connect.
     */
    public String createUrl(boolean createDB)
    {
        // jdbc:oracle:thin:@//${replicator.global.db.host}:${replicator.global.db.port}/${replicator.applier.oracle.service}
        // If we have an URL already just use that.
        boolean useService = serviceName != null && serviceName.trim().length() > 0;

        if (url != null)
            return url;

        // Otherwise compute the MySQL DBMS URL.
        StringBuffer sb = new StringBuffer();
        if (jdbcHeader == null)
            if (useService)
                sb.append("jdbc:oracle:thin:@//");
            else
                sb.append("jdbc:oracle:thin:@");

        else
            sb.append(jdbcHeader);
        sb.append(host);
        sb.append(":");
        sb.append(port);
        if (useService)
        {
            sb.append("/");
            sb.append(serviceName);
        }
        else if (sid != null && sid.length() > 0)
        {
            sb.append(":");
            sb.append(sid);
        }
        if (urlOptions != null && urlOptions.length() > 0)
        {
            // Prepend ? if needed to make the URL options syntactically
            // correct, then add the option string.
            if (!urlOptions.startsWith("?"))
                sb.append("?");
            sb.append(urlOptions);
        }
        return sb.toString();
    }
}