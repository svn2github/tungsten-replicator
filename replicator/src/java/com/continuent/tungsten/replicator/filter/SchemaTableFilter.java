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
 * Initial developer(s): Stephane GIRON
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.filter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.cache.IndexedLRUCache;
import com.continuent.tungsten.replicator.database.TableMatcher;

/**
 * This is the underlying class of the ReplicateFilter. It is also used by the
 * redshift.js applier to filter CDC table.
 * 
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class SchemaTableFilter
{
    private static Logger            logger = Logger.getLogger(SchemaTableFilter.class);

    private TableMatcher             doMatcher;
    private TableMatcher             ignoreMatcher;

    // Cache to look up filtered tables.
    private IndexedLRUCache<Boolean> filterCache;

    public SchemaTableFilter()
    {
        // Initialize LRU cache.
        this.filterCache = new IndexedLRUCache<Boolean>(1000, null);
    }

    public SchemaTableFilter(String schemaTableFilterFilePrefix)
    {
        this();

        logger.info("Loading SchemaTableFilter for "
                + schemaTableFilterFilePrefix);

        prepare(schemaTableFilterFilePrefix);

    }

    private void prepare(String schemaTableFilterFilePrefix)
    {
        // Load and parse whitelist file (schema or table that can be
        // replicated)
        File file = new File(schemaTableFilterFilePrefix + ".do");
        if (file.exists() && file.canRead())
        {
            doMatcher = extractFilter(parse(file));
        }
        else
        {
            logger.info("No accept filter file found ("
                    + schemaTableFilterFilePrefix + ".do)");
        }

        // Load and parse blacklist file (schema or table that should not
        // be replicated)
        file = new File(schemaTableFilterFilePrefix + ".ignore");
        if (file.exists() && file.canRead())
        {
            ignoreMatcher = extractFilter(parse(file));
        }
        else
        {
            logger.info("No ignore filter file found ("
                    + schemaTableFilterFilePrefix + ".ignore)");
        }
    }

    public SchemaTableFilter(String doFilter, String ignoreFilter)
    {
        this();

        doMatcher = extractFilter(doFilter);
        ignoreMatcher = extractFilter(ignoreFilter);
    }

    public void setSchemaTableFilterFilePrefix(
            String schemaTableFilterFilePrefix)
    {
        logger.info("Preparing filter from " + schemaTableFilterFilePrefix);
        prepare(schemaTableFilterFilePrefix);
    }

    private String parse(File file)
    {
        BufferedReader br = null;
        try
        {
            String currentLine;
            StringBuffer buf = new StringBuffer();
            br = new BufferedReader(new FileReader(file));

            while ((currentLine = br.readLine()) != null)
            {
                if (buf.length() > 0)
                    buf.append(",");
                buf.append(currentLine.trim());
            }
            return buf.toString();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        finally
        {
            try
            {
                if (br != null)
                    br.close();
            }
            catch (IOException ex)
            {
                ex.printStackTrace();
            }
        }
        return null;
    }

    // Prepares table matcher.
    private TableMatcher extractFilter(String filter)
    {
        // If empty, we do nothing.
        if (filter == null || filter.length() == 0)
            return null;

        TableMatcher tableMatcher = new TableMatcher();
        tableMatcher.prepare(filter);
        return tableMatcher;
    }

    // Returns true if the schema and table should be filtered using either a
    // cache look-up or a full scan based on filtering rules.
    public boolean filterEvent(String schema, String table)
    {
        // if schema not provided, cannot filter
        if (schema.length() == 0)
            return false;

        // Find out if we need to filter.
        String key = fullyQualifiedName(schema, table);
        Boolean filter = filterCache.get(key);
        if (filter == null)
        {
            filter = filterEventRaw(schema, table);
            filterCache.put(key, filter);
        }

        // Return a value.
        return filter;
    }

    // Performs a scan of all rules to see if we need to filter this event.
    private boolean filterEventRaw(String schema, String table)
    {
        // Check to see if we explicitly ignore this schema/table.
        if (ignoreMatcher != null)
        {
            if (ignoreMatcher.match(schema, table))
                return true;
        }

        // Now to see if we accept this schema/table...
        if (doMatcher == null)
        {
            // If there is no explicit 'do' matcher, we do not filter anything.
            return false;
        }
        else
        {
            // If there is an explicit filter we filter only if we *do not*
            // match.
            return !doMatcher.match(schema, table);
        }
    }

    // Returns the fully qualified schema and/or table name, which can be used
    // as a key.
    public String fullyQualifiedName(String schema, String table)
    {
        StringBuffer fqn = new StringBuffer();
        fqn.append(schema);
        if (table != null)
            fqn.append(".").append(table);
        return fqn.toString();
    }

    public void release()
    {
        if (filterCache != null)
            this.filterCache.invalidateAll();
    }

}
