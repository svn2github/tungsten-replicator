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
 * Initial developer(s): Stephane Giron
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.extractor.parallel;

import java.util.List;

import com.continuent.tungsten.replicator.database.Table;

/**
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class StringChunk implements Chunk
{

    private Table  table;
    private String min;
    private String max;
    private long   nbBlocks = 0;

    public StringChunk(Table table, String min, String max)
    {
        this.table = table;
        this.min = min;
        this.max = max;
    }

    /**
     * Creates a new <code>StringChunk</code> object
     * 
     * @param table
     * @param min
     * @param max
     * @param nbBlocks
     */
    public StringChunk(Table table, String min, String max, long nbBlocks)
    {
        this(table, min, max);
        this.nbBlocks = nbBlocks;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.parallel.Chunk#getTable()
     */
    @Override
    public Table getTable()
    {
        // TODO Auto-generated method stub
        return table;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.parallel.Chunk#getColumns()
     */
    @Override
    public List<String> getColumns()
    {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.parallel.Chunk#getFrom()
     */
    @Override
    public String getFrom()
    {
        // TODO Auto-generated method stub
        return min;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.parallel.Chunk#getTo()
     */
    @Override
    public Object getTo()
    {
        // TODO Auto-generated method stub
        return max;
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

    @Override
    public String getWhereClause()
    {
        if (getFrom() != null)
        {
            StringBuffer sql = new StringBuffer(" WHERE ");
            String pkName = getTable().getPrimaryKey().getColumns().get(0)
                    .getName();

            sql.append(pkName);
            sql.append(" >= '");
            sql.append(getFrom());
            sql.append("' AND ");
            sql.append(pkName);
            sql.append(" <= '");
            sql.append(getTo());
            sql.append("'");
        }
        return null;
    }

}
