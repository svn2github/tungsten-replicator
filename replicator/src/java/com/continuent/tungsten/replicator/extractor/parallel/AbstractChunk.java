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

import java.util.Iterator;
import java.util.List;

import com.continuent.tungsten.replicator.database.Column;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.Table;

/**
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public abstract class AbstractChunk implements Chunk
{

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.parallel.Chunk#getTable()
     */
    @Override
    public abstract Table getTable();

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.parallel.Chunk#getColumns()
     */
    @Override
    public abstract List<String> getColumns();

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.parallel.Chunk#getFrom()
     */
    @Override
    public abstract Object getFrom();

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.parallel.Chunk#getTo()
     */
    @Override
    public abstract Object getTo();

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.parallel.Chunk#getNbBlocks()
     */
    @Override
    public abstract long getNbBlocks();

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.parallel.Chunk#getWhereClause()
     */
    protected abstract String getWhereClause();

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.parallel.Chunk#getQuery()
     */
    @Override
    public String getQuery(Database connection, String eventId)
    {
        StringBuffer sql = new StringBuffer();

        List<String> columns = getColumns();
        if (columns == null)
            for (Column column : getTable().getAllColumns())
            {
                if (sql.length() == 0)
                {
                    sql.append("SELECT ");
                }
                else
                {
                    sql.append(", ");
                }
                sql.append(column.getName());
            }
        else
            for (Iterator<String> iterator = columns.iterator(); iterator
                    .hasNext();)
            {
                if (sql.length() == 0)
                {
                    sql.append("SELECT ");
                }
                else
                {
                    sql.append(", ");
                }
                sql.append(iterator.next());
            }

        sql.append(" FROM ");
        sql.append(connection.getDatabaseObjectName(getTable().getSchema()));
        sql.append('.');
        sql.append(connection.getDatabaseObjectName(getTable().getName()));

        if (eventId != null)
        {
            sql.append(" AS OF SCN ");
            sql.append(eventId);
        }

        String where = getWhereClause();
        if (where != null)
            sql.append(where);

        return sql.toString();
    }

}
