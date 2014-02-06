/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2009 Continuent Inc.
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
 * Initial developer(s): Stephane Giron
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.extractor.parallel;

import java.util.Arrays;
import java.util.List;

import com.continuent.tungsten.replicator.database.Table;

/**
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class NumericChunk
{
    private Number       from;

    private Number       to;

    private Table        table;

    private List<String> columns;

    private long         nbBlocks;

    public NumericChunk(Table table, Number from, Number to, String[] columns)
    {
        this.table = table;
        this.from = from;
        this.to = to;
        if (columns == null)
            this.columns = null;
        else
            this.columns = Arrays.asList(columns);
    }

    public NumericChunk(Table table, Number from, Number to, String[] columns,
            long nbBlocks)
    {
        this(table, from, to, columns);
        this.nbBlocks = nbBlocks;
    }

    public NumericChunk(Table table, String[] columns)
    {
        this(table, -1, -1, columns);
        this.nbBlocks = 1;
    }

    public NumericChunk()
    {
        // Generate an empty chunk that will tell threads that work is complete
        this.table = null;
    }

    /**
     * Returns the columns value.
     * 
     * @return Returns the columns.
     */
    protected List<String> getColumns()
    {
        return columns;
    }

    /**
     * Returns the from value.
     * 
     * @return Returns the from.
     */
    protected long getFrom()
    {
        return from.longValue();
    }

    /**
     * Returns the nbBlocks value.
     * 
     * @return Returns the nbBlocks.
     */
    protected long getNbBlocks()
    {
        return nbBlocks;
    }

    public Table getTable()
    {
        return table;
    }

    /**
     * Returns the to value.
     * 
     * @return Returns the to.
     */
    protected long getTo()
    {
        return to.longValue();
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        return "Chunk for table " + table.getSchema() + "." + table.getName()
                + " for " + table.getPrimaryKey().getColumns().get(0).getName()
                + " from " + from + " to " + to;
    }

}
