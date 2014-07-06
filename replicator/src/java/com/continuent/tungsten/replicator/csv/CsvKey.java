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

package com.continuent.tungsten.replicator.csv;

/**
 * Defines a key for a CSV file with a CSV file set. A key is a value used to
 * partition CSV writes.
 */
public class CsvKey implements Comparable<CsvKey>
{
    private static final CsvKey emptyKey = new CsvKey("");

    public String               key;

    /**
     * Instantiates a new instance.
     * 
     * @param stagePkeyColumn Name of the primary key column
     */
    public CsvKey(String key)
    {
        this.key = key;
    }

    /**
     * Returns a standard key for cases where there is only a single key used.
     * This enables clients to use the same code path regardless of whether
     * distributed by keys are used.
     */
    public static CsvKey emptyKey()
    {
        return emptyKey;
    }

    /** Returns true if this is the empty key. */
    public boolean isEmptyKey()
    {
        return "".equals(key);
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#toString()
     */
    public String toString()
    {
        return key;
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    public int compareTo(CsvKey anotherKey)
    {
        return key.compareTo((String) anotherKey.toString());
    }
}