/**
 * Tungsten: An Application Server for uni/cluster.
 * Copyright (C) 2011 Continuent Inc.
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

package com.continuent.tungsten.commons.cache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Implements a self-managing cache suitable for database metadata. The cache
 * has a specific capacity which is maintained by invalidating cache nodes
 * implicitly using an LRU (least recently used) algorithm. Clients can also
 * invalidate individual keys, key ranges, and the entire cache.
 * <p/>
 * NOTE: The cache is designed for use by a single thread. There is no
 * synchronization, nor do we have a notion of "loans."
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class IndexedLRUCache<T>
{
    // Hash index on cache nodes.
    private Map<String, CacheNode<T>> pmap = new HashMap<String, CacheNode<T>>();

    // Most and leads recently used node.
    private CacheNode<T>              lruFirst;
    private CacheNode<T>              lruLast;

    // Maximum size of the cache.
    private int                       capacity;

    // Call-back to release values.
    private CacheResourceManager<T>   resourceManager;

    /**
     * Creates a new prepared statement cache.
     * 
     * @param Maximum capacity of the cache, after which nodes are removed in
     *            LRU order
     */
    public IndexedLRUCache(int capacity, CacheResourceManager<T> resourceManager)
    {
        this.capacity = capacity;
        this.resourceManager = resourceManager;
    }

    /**
     * Return current size of list.
     */
    public int size()
    {
        return pmap.size();
    }

    /**
     * Store a value in the cache.
     */
    public void put(String key, T value)
    {
        // If there is a previous node, unlink and release it.
        CacheNode<T> old = pmap.get(key);
        if (old != null)
            remove(old);

        // If the index is at capacity, unlink the least recently used node.
        if (pmap.size() >= capacity)
            remove(lruLast);

        // Add the new node.
        CacheNode<T> node = new CacheNode<T>(key, value);
        add(node);
    }

    /**
     * Retrieves a value or returns null if it is not in the cache.
     */
    public T get(String key)
    {
        CacheNode<T> node = pmap.get(key);
        if (node == null)
            return null;
        else
        {
            // Relink node in the LRU.
            unlink(node);
            link(node);
            return node.get();
        }
    }

    /**
     * Returns all current keys.
     */
    public Set<String> keys()
    {
        Set<String> keys = new HashSet<String>();
        keys.addAll(pmap.keySet());
        return keys;
    }

    /**
     * Returns values in LRU order.
     */
    public List<T> lruValues()
    {
        ArrayList<T> lruValues = new ArrayList<T>(pmap.size());
        CacheNode<T> next = lruFirst;
        while (next != null)
        {
            lruValues.add(next.get());
            next = next.getAfter();
        }
        return lruValues;
    }

    /**
     * Invalidate all values in the cache, returning number of items deleted.
     */
    public int invalidateAll()
    {
        int deleted = this.pmap.size();
        for (String key : keys())
            invalidate(key);
        return deleted;
    }

    /**
     * Invalidate all values that start with the given key prefix, returning number
     * deleted.
     */
    public int invalidateByPrefix(String prefix)
    {
        int deleted = 0;
        for (String key : keys())
        {
            if (key.startsWith(prefix))
            {
                invalidate(key);
                deleted++;
            }
        }
        return deleted;
    }

    /**
     * Release the value corresponding to a specific key, returning the number
     * of items deleted.
     */
    public int invalidate(String key)
    {
        CacheNode<T> node = pmap.get(key);
        if (node != null)
        {
            remove(node);
            return 1;
        }
        else
            return 0;
    }

    // Link a node into the index, which means to insert the same in the
    // hash table and add it to the head of the LRU list.
    private void add(CacheNode<T> node)
    {
        pmap.put(node.getKey(), node);
        link(node);
    }

    // Free a node from the LRU chain, which means remove from hash index,
    // unlink from LRU chain, and free the resource.
    private void remove(CacheNode<T> node)
    {
        pmap.remove(node.getKey());
        unlink(node);
        if (resourceManager != null)
            resourceManager.release(node.get());
    }

    // Link node into the head of the LRU list.
    private void link(CacheNode<T> node)
    {
        if (lruFirst == null)
        {
            // First node in an empty list.
            node.setAfter(null);
            node.setBefore(null);
            lruFirst = node;
            lruLast = node;
        }
        else
        {
            // Adding to list with >=1 members.
            lruFirst.setBefore(node);
            node.setAfter(lruFirst);
            node.setBefore(null);
            lruFirst = node;
        }
    }

    // Free a node from the LRU list.
    private void unlink(CacheNode<T> node)
    {
        // Unlink from the LRU list.
        CacheNode<T> before = node.getBefore();
        CacheNode<T> after = node.getAfter();

        if (before == null)
            lruFirst = after;
        else
            before.setAfter(after);

        if (after == null)
            lruLast = before;
        else
            after.setBefore(before);
    }
}