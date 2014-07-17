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
public class SqlConnectionSpecMySQL extends SqlConnectionSpecGeneric
{
    // Extra properties supported by MySQL connections.
    protected String jdbcHeader;
    protected String urlOptions;

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

    /**
     * Generates a MySQL URL with or without the createDB=true option. This
     * option should *only* be used the first time we connect.
     */
    public String createUrl(boolean createDB)
    {
        // If we have an URL already just use that.
        if (url != null)
            return url;

        // Otherwise compute the MySQL DBMS URL.
        StringBuffer sb = new StringBuffer();
        if (jdbcHeader == null)
            sb.append("jdbc:mysql:thin://");
        else
            sb.append(jdbcHeader);
        sb.append(host);
        sb.append(":");
        sb.append(port);
        sb.append("/");
        sb.append(schema);
        if (urlOptions != null && urlOptions.length() > 0)
        {
            // Prepend ? if needed to make the URL options syntactically
            // correct, then add the option string.
            if (!urlOptions.startsWith("?"))
                sb.append("?");
            sb.append(urlOptions);

            if (createDB)
            {
                sb.append("&createDB=true");
            }

            if (sslEnabled)
            {
                sb.append("&useSSL=true");
            }
        }
        else if (createDB)
        {
            sb.append("?createDB=true");
            if (sslEnabled)
            {
                sb.append("&useSSL=true");
            }
        }
        else if (sslEnabled)
        {
            sb.append("?useSSL=true");
        }
        return sb.toString();
    }
}