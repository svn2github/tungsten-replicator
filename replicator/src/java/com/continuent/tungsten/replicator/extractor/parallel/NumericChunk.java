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
public class NumericChunk implements Chunk
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
        this(table, null, null, columns);
        this.nbBlocks = 1;
    }

    public NumericChunk()
    {
        // Generate an empty chunk that will tell threads that work is complete
        this.table = null;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.parallel.Chunk#getColumns()
     */
    @Override
    public List<String> getColumns()
    {
        return columns;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.parallel.Chunk#getFrom()
     */
    @Override
    public Long getFrom()
    {
        if (from == null)
            return null;
        return from.longValue();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.parallel.Chunk#getNbBlocks()
     */
    @Override
    public long getNbBlocks()
    {
        return nbBlocks;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.parallel.Chunk#getTable()
     */
    @Override
    public Table getTable()
    {
        return table;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.parallel.Chunk#getTo()
     */
    @Override
    public Long getTo()
    {
        if (to == null)
            return null;
        return to.longValue();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.parallel.Chunk#toString()
     */
    @Override
    public String toString()
    {
        return "Chunk for table " + table.getSchema() + "." + table.getName()
                + " for " + table.getPrimaryKey().getColumns().get(0).getName()
                + " from " + from + " to " + to;
    }

    @Override
    public String getWhereClause()
    {
        if (getFrom() != null)
        {
            StringBuffer sql = new StringBuffer(" WHERE ");
            String pkName = getTable().getPrimaryKey().getColumns().get(0)
                    .getName();

            sql.append(pkName);
            sql.append(" > ");
            sql.append(getFrom());
            sql.append(" AND ");
            sql.append(pkName);
            sql.append(" <= ");
            sql.append(getTo());
            return sql.toString();
        }
        return null;
    }
}
