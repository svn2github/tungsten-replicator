/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2014 Continuent Inc.
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
 * Initial developer(s): Linas Virbalas
 * Contributor(s):  
 */

package com.continuent.tungsten.replicator.datatypes;

import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnSpec;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnVal;

/**
 * This class represents a numeric value saved in THL event.
 * 
 * @author <a href="mailto:linas.virbalas@continuent.com">Linas Virbalas</a>
 * @version 1.0
 */
public class Numeric
{
    boolean    isNegative   = false;
    Long       extractedVal = null;
    ColumnSpec columnSpec   = null;

    public Numeric(ColumnSpec columnSpec, ColumnVal value)
    {
        this.columnSpec = columnSpec;
        if (value.getValue() instanceof Integer)
        {
            int val = (Integer) value.getValue();
            isNegative = val < 0;
            extractedVal = Long.valueOf(val);
        }
        else if (value.getValue() instanceof Long)
        {
            long val = (Long) value.getValue();
            isNegative = val < 0;
            extractedVal = Long.valueOf(val);
        }
    }

    /**
     * Is the number negative?
     */
    public boolean isNegative()
    {
        return isNegative;
    }

    /**
     * Representation of the numeric value in Long format.
     * 
     * @return null, if value couldn't be extracted (type mismatch?).
     */
    public Long getExtractedValue()
    {
        return extractedVal;
    }

    /**
     * Returns column specification used to create this numeric object.
     */
    public ColumnSpec getColumnSpec()
    {
        return columnSpec;
    }
}
