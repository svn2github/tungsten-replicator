/**
 * Tungsten: An Application Server for uni/cluster.
 * Copyright (C) 2012 Continuent Inc.
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

package com.continuent.tungsten.replicator.database;

import java.sql.PreparedStatement;

/**
 * Holds a single prepared statement in a form suitable for caches. We keep the
 * original statement for future reference.
 */
public class PreparedStatementHolder
{
    private final String            key;
    private final PreparedStatement preparedStatement;
    private final String            query;

    /**
     * Create a new holder for a prepared statement.
     * 
     * @param key Key to look up this prepared statement
     * @param preparedStatement Prepared statement
     * @param query Text of the prepared statement query
     */
    public PreparedStatementHolder(String key,
            PreparedStatement preparedStatement, String query)
    {
        this.key = key;
        this.preparedStatement = preparedStatement;
        this.query = query;
    }

    public synchronized String getKey()
    {
        return key;
    }

    public synchronized PreparedStatement getPreparedStatement()
    {
        return preparedStatement;
    }

    public synchronized String getQuery()
    {
        return query;
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#toString()
     */
    public String toString()
    {
        StringBuffer sb = new StringBuffer(this.getClass().getSimpleName());
        sb.append(" key=").append(key);
        sb.append(" query=").append(query);
        return sb.toString();
    }
}