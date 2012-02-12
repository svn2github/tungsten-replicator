/**
 * Tungsten Scale-Out Stack
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

package com.continuent.tungsten.replicator.prefetch;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.continuent.tungsten.replicator.dbms.OneRowChange;

/**
 * Data for a single row, regardless of whether before or after the change.
 */
public class RbrRowImage
{
    /** Images can be one of two types. */
    public enum ImageType
    {
        /** Row values prior to change. */
        BEFORE,
        /** Row values after change. */
        AFTER
    }

    // Properties.
    private final ImageType                      type;
    private final RbrTableChangeSet              changeSet;
    private final List<OneRowChange.ColumnVal>   values;
    private final List<OneRowChange.ColumnSpec>  specs;

    // Index of column names.
    private Map<String, OneRowChange.ColumnSpec> colNames;

    /** Creates a row image with the minimum effort required. */
    public RbrRowImage(ImageType type, RbrTableChangeSet changeSet,
            List<OneRowChange.ColumnSpec> specs,
            List<OneRowChange.ColumnVal> values)
    {
        this.type = type;
        this.changeSet = changeSet;
        this.specs = specs;
        this.values = values;
    }

    /** Returns the image type. */
    public ImageType getType()
    {
        return type;
    }

    // Delegated methods to return schema and table names.
    public String getSchemaName()
    {
        return changeSet.getSchemaName();
    }

    public String getTableName()
    {
        return changeSet.getTableName();
    }

    /**
     * Look up the index of a column name, returning -1 if it is not present.
     * Index values start at 1 for first column.
     */
    public int getColumnIndex(String name)
    {
        if (colNames == null)
        {
            colNames = new HashMap<String, OneRowChange.ColumnSpec>(size());
            for (OneRowChange.ColumnSpec spec : specs)
            {
                colNames.put(spec.getName(), spec);
            }
        }
        OneRowChange.ColumnSpec spec = colNames.get(name);
        if (spec == null)
            return -1;
        else
            return spec.getIndex();
    }

    /**
     * Return a specific column specification by index, where index starts at 1
     * for first column.
     */
    OneRowChange.ColumnSpec getSpec(int index)
    {
        return specs.get(index - 1);
    }

    /** Return a specific column specification by name. */
    OneRowChange.ColumnSpec getSpec(String name)
    {
        int colIndex = getColumnIndex(name);
        if (colIndex == -1)
            return null;
        else
            return getSpec(colIndex);
    }

    /**
     * Return a specific column value by index, where index starts at 1 for
     * first column.
     */
    OneRowChange.ColumnVal getValue(int index)
    {
        return values.get(index - 1);
    }

    /** Return a specific column value by name. */
    OneRowChange.ColumnVal getValue(String name)
    {
        int colIndex = getColumnIndex(name);
        if (colIndex == -1)
            return null;
        else
            return getValue(colIndex);
    }

    /** Return number of columns. */
    int size()
    {
        return specs.size();
    }
}