/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2010 Continuent Inc.
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
 * Contributor(s): Gilles Rayrat
 */

package com.continuent.tungsten.common.cluster.resource;

import java.sql.Connection;
import java.sql.SQLException;

import com.continuent.tungsten.common.patterns.order.Sequence;
import com.continuent.tungsten.common.utils.CLUtils;

public class DatabaseConnection
{
    public enum ConnectionType
    {
        DIRECT, CLUSTER, CONNECTOR
    };

    private ConnectionType type     = ConnectionType.DIRECT;
    private String         name;
    private Connection     connection;
    private Sequence       sequence = null;
    private Object         context;

    public DatabaseConnection(ConnectionType type, String name,
            Connection connection, Object context)
    {
        this.type = type;
        this.name = name;
        this.connection = connection;
        setContext(context);
    }

    public ConnectionType getType()
    {
        return type;
    }

    public void setType(ConnectionType type)
    {
        this.type = type;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public Connection getConnection()
    {
        return connection;
    }

    public Object getContext()
    {
        return context;
    }

    public void setContext(Object context)
    {
        this.context = context;

        if (context instanceof DataSource)
        {
            this.sequence = ((DataSource) context).getSequence();

        }
    }

    public Sequence getSequence()
    {
        return sequence;
    }

    public void setSequence(Sequence sequence)
    {
        this.sequence = sequence;
    }

    public DataSource getDs()
    {
        if (type == ConnectionType.DIRECT)
        {
            return (DataSource) context;
        }

        return null;
    }

    public String toString()
    {
        return toString(false);
    }

    public String toString(boolean detailed)
    {
        if (type == ConnectionType.DIRECT)
        {
            DataSource ds = (DataSource) getContext();
            return String.format("%s(%s) DIRECT TO %s", name, liveness(),
                    ds.toString());
        }
        else if (type == ConnectionType.CLUSTER)
        {
            StringBuilder builder = new StringBuilder();
            builder.append(name).append('(').append(liveness())
                    .append(") VIA CLUSTER TO ").append(connection.toString());
            return builder.toString();
        }
        else if (type == ConnectionType.CONNECTOR)
        {
            return String.format("%s(%s) CONNECTOR TO HOST %s", name,
                    liveness(), getContext());
        }
        else
        {
            CLUtils.println(String.format(
                    "no connection status logic for type %s", type));
            return "UNKNOWN";
        }
    }

    /**
     * Provides a string representation of whether this connection is closed or
     * not
     * 
     * @return "CLOSED" if the wrapped JDBC connection is closed, "OPEN"
     *         otherwise
     */
    private String liveness()
    {
        boolean isClosed = false;

        try
        {
            if (isClosed())
            {
                isClosed = true;
            }
        }
        catch (SQLException s)
        {
            isClosed = true;
        }

        if (isClosed)
        {
            return "CLOSED";
        }

        return "OPEN";
    }

    public boolean isClosed() throws SQLException
    {
        return connection.isClosed();
    }
}
