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

import com.continuent.tungsten.replicator.dbms.OneRowChange;

/**
 * Encapsulates changes on a single row in a single table, providing access to
 * the before and after images of the row data.
 */
public class RbrRowChange
{
    private final RbrTableChangeSet changeSet;
    private final int               index;

    /** Instantiates a single row change. */
    RbrRowChange(RbrTableChangeSet changeSet, int index)
    {
        this.changeSet = changeSet;
        this.index = index;
    }

    // Delegate methods on change set.
    public boolean isInsert()
    {
        return changeSet.isInsert();
    }

    public boolean isUpdate()
    {
        return changeSet.isUpdate();
    }

    public boolean isDelete()
    {
        return changeSet.isDelete();
    }

    public String getSchemaName()
    {
        return changeSet.getSchemaName();
    }

    public String getTableName()
    {
        return changeSet.getTableName();
    }

    /**
     * Returns the before image as a separate object or null if it does not
     * exist.
     */
    public RbrRowImage getBeforeImage()
    {
        OneRowChange oneRowChange = changeSet.getOneRowChange();
        if (isInsert())
        {
            // Inserts have no before data.
            return null;
        }
        else
        {
            // For deletes and updates the keys.
            return new RbrRowImage(RbrRowImage.ImageType.BEFORE,
                    this.changeSet, oneRowChange.getKeySpec(), oneRowChange
                            .getKeyValues().get(index));
        }
    }

    /**
     * Returns the before image as a separate object or null if it does not
     * exist;
     */
    public RbrRowImage getAfterImage()
    {
        OneRowChange oneRowChange = changeSet.getOneRowChange();
        if (isDelete())
        {
            // Deletes have no after data.
            return null;
        }
        else
        {
            // For inserts and updates take the values.
            return new RbrRowImage(RbrRowImage.ImageType.AFTER, this.changeSet,
                    oneRowChange.getColumnSpec(), oneRowChange
                            .getColumnValues().get(index));
        }
    }

    /** Return number of columns. */
    int size()
    {
        OneRowChange oneRowChange = changeSet.getOneRowChange();
        if (isDelete())
            return oneRowChange.getKeyValues().size();
        else
            return oneRowChange.getColumnValues().size();
    }
}