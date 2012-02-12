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
 * Wrapper for a list of row changes to a set of tables, each of which is
 * represented by a table change set.
 */
public class RbrChangeList
{
    private final RowChangeData rowChangeData;

    /** Instantiates a new row change. */
    public RbrChangeList(RowChangeData rowChangeData)
    {
        this.rowChangeData = rowChangeData;
    }

    /** Return all changes from this set in a newly instantiated list. */
    public List<RbrTableChangeSet> getChanges()
    {
        List<RbrTableChangeSet> changes = new ArrayList<RbrTableChangeSet>(
                this.size());
        for (OneRowChange rowChanges : rowChangeData.getRowChanges())
        {
            changes.add(new RbrTableChangeSet(rowChanges));
        }
        return changes;
    }

    /** Returns a single row change set. */
    public RbrTableChangeSet getChange(int index)
    {
        return new RbrTableChangeSet(rowChangeData.getRowChanges().get(index));
    }

    /** Return number of row changes. */
    public int size()
    {
        return rowChangeData.getRowChanges().size();
    }
}