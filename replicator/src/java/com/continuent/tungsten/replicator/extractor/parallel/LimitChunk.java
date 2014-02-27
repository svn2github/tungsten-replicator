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
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class LimitChunk extends AbstractChunk implements Chunk
{

    private Table table;
    private long  from;
    private long  to;
    private long  nbBlocks = 0;

    public LimitChunk(Table table)
    {
        this.table = table;
    }

    public LimitChunk(Table table, long from, long to, long nbBlocks)
    {
        this(table);
        this.from = from;
        this.to = to;
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
        // TODO Auto-generated method stub
        return nbBlocks;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.parallel.AbstractChunk#getQuery(com.continuent.tungsten.replicator.database.Database)
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

        sql.append("SELECT * FROM ");
        sql.append("( SELECT subQuery.*, ROWNUM rnum from ( SELECT ");
        sql.append(colsList);
        sql.append(" FROM ");
        sql.append(fqnTable);
        if (eventId != null)
        {
            sql.append(" AS OF SCN ");
            sql.append(eventId);
        }
        sql.append(" ORDER BY ");
        if (keysList.length() > 0)
            sql.append(keysList);
        else
            sql.append(colsList);
        sql.append(") subQuery where ROWNUM <= ");
        sql.append(this.to);
        sql.append(" ) where rnum >= ");
        sql.append(this.from);

        return sql.toString();
    }

    @Override
    protected String getWhereClause()
    {
        // StringBuilder sql = new StringBuilder();
        // ArrayList<Column> allColumns = table.getAllColumns();
        // String[] colList = new String[allColumns.size()];
        //
        // for (Column column : allColumns)
        // {
        // colList[column.getPosition()] = column.getName();
        // }
        //
        // for (int i = 0; i < colList.length; i++)
        // {
        // if (sql.length() == 0)
        // sql.append(" ORDER BY ");
        // else
        // sql.append(", ");
        // sql.append(colList[i]);
        //
        // }
        //
        //
        // StringBuffer sqlBuf = new StringBuffer("SELECT MIN(");
        // sqlBuf.append(pkName);
        // sqlBuf.append(") as min, MAX(");
        // sqlBuf.append(pkName);
        // sqlBuf.append(") as max, COUNT(");
        // sqlBuf.append(pkName);
        // sqlBuf.append(") as cnt FROM
        //
        // return sql.toString();
        return null;
    }
}
