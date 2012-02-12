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
import java.sql.SQLException;

import com.continuent.tungsten.commons.cache.CacheResourceManager;
import com.continuent.tungsten.commons.cache.IndexedLRUCache;

/**
 * Implements a cache for prepared statements, which are identified by a single
 * key.
 */
public class PreparedStatementCache
        implements
            CacheResourceManager<PreparedStatementHolder>
{
    IndexedLRUCache<PreparedStatementHolder> cache;

    /**
     * Creates a new table metadata cache.
     */
    public PreparedStatementCache(int capacity)
    {
        cache = new IndexedLRUCache<PreparedStatementHolder>(capacity, this);
    }

    public void release(PreparedStatementHolder psh)
    {
        try
        {
            if (psh.getPreparedStatement() != null)
            {
                psh.getPreparedStatement().close();
            }
        }
        catch (SQLException e)
        {
        }
    }

    /**
     * Returns the number of entries in the metadata cache.
     */
    public int size()
    {
        return cache.size();
    }

    /**
     * Store prepared statement.
     */
    public void store(String key, PreparedStatement ps, String query)
    {
        PreparedStatementHolder psh = new PreparedStatementHolder(key, ps,
                query);
        cache.put(key, psh);
    }

    /**
     * Retrieves prepared statement or returns null if it is not in the cache.
     */
    public PreparedStatement retrieve(String key)
    {
        PreparedStatementHolder psh = retrieveExtended(key);
        if (psh == null)
            return null;
        else
            return psh.getPreparedStatement();
    }

    /**
     * Retrieves prepared statement plus query text or returns null if not in
     * the cache.
     */
    public PreparedStatementHolder retrieveExtended(String key)
    {
        return cache.get(key);
    }

    /**
     * Release one prepared statement.
     */
    public void invalidate(String key)
    {
        cache.invalidate(key);
    }

    /**
     * Release all metadata in the cache.
     */
    public void invalidateAll()
    {
        cache.invalidateAll();
    }
}