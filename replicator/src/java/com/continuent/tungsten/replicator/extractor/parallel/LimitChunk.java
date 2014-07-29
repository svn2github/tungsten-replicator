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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.continuent.tungsten.replicator.database.Column;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.Key;
import com.continuent.tungsten.replicator.database.Table;

/**
 * Defines a chunk based on limits (starting and ending points). These limits
 * are based on primary key or on a good unique key candidate.
 * 
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class LimitChunk extends AbstractChunk implements Chunk
{

    private Table    table;
    private long     from;
    private long     nbBlocks   = 0;
    private Object[] fromValues = null;
    private Object[] toValues;

    public LimitChunk(Table table)
    {
        this.table = table;
    }

    private LimitChunk(Table table, long from, long to, long nbBlocks)
    {
        this(table);
        this.from = from;
        this.nbBlocks = nbBlocks;
    }

    // TODO : clean up useless parameters from here (from, to, whereclause,
    // chunksize)
    public LimitChunk(Table table, long from, long to, long nbBlocks,
            Object[] fromValues, Object[] toValues, String whereClause,
            long chunkSize)
    {
        this(table, from, to, nbBlocks);
        this.fromValues = fromValues;
        this.toValues = toValues;
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
     * @see com.continuent.tungsten.replicator.extractor.parallel.Chunk#getColumns()
     */
    @Override
    public List<String> getColumns()
    {
        return null;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.parallel.Chunk#getFrom()
     */
    @Override
    public Object getFrom()
    {
        // TODO Auto-generated method stub
        return from;
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
        return null;
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
     */
    @Override
    public String getQuery(Database connection, String eventId)
    {
        String fqnTable = connection.getDatabaseObjectName(table.getSchema())
                + '.' + connection.getDatabaseObjectName(table.getName());

        StringBuilder sql = new StringBuilder();

        StringBuilder colsList = new StringBuilder(), keysList = new StringBuilder();
        List<String> columns = getColumns();

        ArrayList<Column> pkColumns = null;
        if (table.getPrimaryKey() != null)
        {
            pkColumns = table.getPrimaryKey().getColumns();
        }
        else
        {
            Key candidatePK = table.getPKFromUniqueIndex();
            if (candidatePK != null)
                pkColumns = candidatePK.getColumns();
        }

        if (columns == null)
            for (Column column : getTable().getAllColumns())
            {
                if (colsList.length() > 0)
                {
                    colsList.append(", ");
                }
                colsList.append(column.getName());
            }
        else
            for (Iterator<String> iterator = columns.iterator(); iterator
                    .hasNext();)
            {
                if (colsList.length() > 0)
                {
                    colsList.append(", ");
                }
                colsList.append(iterator.next());
            }

        if (pkColumns != null)
            for (Iterator<Column> iterator = pkColumns.iterator(); iterator
                    .hasNext();)
            {
                if (keysList.length() > 0)
                {
                    keysList.append(", ");
                }
                keysList.append(iterator.next().getName());
            }

        sql.append("SELECT * ");
        sql.append(" FROM ");
        sql.append(fqnTable);

        sql.append(AbstractChunk.getFlashbackQueryClause(connection, eventId));

        if (fromValues != null || toValues != null)
        {
            sql.append(" WHERE ");
        }

        if (fromValues != null)
        {
            sql.append("(");
            sql.append(buildFromWhereClause(pkColumns.toArray(), 0));
            sql.append(")");
        }

        if (toValues != null)
        {
            if (fromValues != null)
                sql.append(" AND ");
            sql.append("(");
            sql.append(buildToWhereClause(pkColumns.toArray(), 0));
            sql.append(")");
        }

        return sql.toString();
    }

    private String buildFromWhereClause(Object[] columns, int index)
    {
        if (index == columns.length - 1)
        {
            return ((Column) columns[index]).getName() + " > ? ";
        }
        else
            return ((Column) columns[index]).getName() + " > ? OR ("
                    + ((Column) columns[index]).getName() + " = ? AND ("
                    + buildFromWhereClause(columns, index + 1) + "))";
    }

    private String buildToWhereClause(Object[] columns, int index)
    {
        if (index == columns.length - 1)
        {
            return ((Column) columns[index]).getName() + " <= ? ";
        }
        else
            return ((Column) columns[index]).getName() + " < ? OR ("
                    + ((Column) columns[index]).getName() + " = ? AND ("
                    + buildToWhereClause(columns, index + 1) + "))";
    }

    @Override
    protected String getWhereClause()
    {
        return null;
    }

    @Override
    public Object[] getFromValues()
    {
        return fromValues;
    }

    @Override
    public Object[] getToValues()
    {
        return toValues;
    }

}
