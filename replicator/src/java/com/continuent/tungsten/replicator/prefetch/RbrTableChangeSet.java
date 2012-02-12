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

import java.util.ArrayList;
import java.util.List;

import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.RowChangeData;

/**
 * Wrapper for a set of changes on a single table. All changes share the same
 * table as well as change type.
 */
public class RbrTableChangeSet
{
    private final OneRowChange oneRowChange;

    /** Instantiates a single table change set. */
    RbrTableChangeSet(OneRowChange oneRowChange)
    {
        this.oneRowChange = oneRowChange;
    }

    /** Returns true if this is an insert. */
    public boolean isInsert()
    {
        return oneRowChange.getAction() == RowChangeData.ActionType.INSERT;
    }

    /** Returns true if this is an update. */
    public boolean isUpdate()
    {
        return oneRowChange.getAction() == RowChangeData.ActionType.UPDATE;
    }

    /** Returns true if this is a delete. */
    public boolean isDelete()
    {
        return oneRowChange.getAction() == RowChangeData.ActionType.DELETE;
    }

    /** Returns the schema to which underlying table applies. */
    public String getSchemaName()
    {
        return oneRowChange.getSchemaName();
    }

    /** Returns the table to change applies. */
    public String getTableName()
    {
        return oneRowChange.getTableName();
    }

    /** Return number of rows. */
    int size()
    {
        if (isDelete())
            return oneRowChange.getKeyValues().size();
        else
            return oneRowChange.getColumnValues().size();
    }

    /** Return all row changes. */
    public List<RbrRowChange> getRowChanges()
    {
        int size = size();
        ArrayList<RbrRowChange> changes = new ArrayList<RbrRowChange>(size);
        for (int i = 0; i < size; i++)
        {
            changes.add(new RbrRowChange(this, i));
        }
        return changes;
    }

    /**
     * Return a single row change.
     */
    public RbrRowChange getRowChange(int index)
    {
        return new RbrRowChange(this, index);
    }

    /**
     * Returns the underlying source data.
     */
    OneRowChange getOneRowChange()
    {
        return oneRowChange;
    }
}