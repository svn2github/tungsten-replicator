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

import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.Table;

/**
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public interface Chunk
{

    /**
     * Returns the table which is concerned by this chunk.
     * 
     * @return Returns the Table this chunk is based on.
     */
    public Table getTable();

    /**
     * Returns the list of columns that are going to be extracted.
     * 
     * @return Returns the list of columns.
     */
    public List<String> getColumns();

    /**
     * Returns the from value.
     * 
     * @return Returns the from.
     */
    public Object getFrom();

    /**
     * Returns the to value.
     * 
     * @return Returns the to.
     */
    public Object getTo();

    /**
     * Returns the nbBlocks value. This is the total number of chunks that will
     * be used to extract the whole table content. It is used to know when a
     * table was fully processed.
     * 
     * @return Returns the nbBlocks.
     */
    public long getNbBlocks();

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString();

    /**
     * Returns the query used to get the data of this chunk.
     * 
     * @param connection Database object used to generate the query.
     * @param eventId Position used for flashback queries (when supported)
     * @return Returns the query to execute to get data of this chunk from the
     *         database, eventually at the given position
     */
    public String getQuery(Database connection, String eventId);

    /**
     * Returns the list of values to be used as starting point of the chunk.
     * 
     * @return Returns the list of values to be used as starting point of the
     *         chunk.
     */
    public Object[] getFromValues();

    /**
     * Returns the list of values to be used as ending point of the chunk.
     * 
     * @return Returns the list of values to be used as ending point of the
     *         chunk.
     */
    public Object[] getToValues();
}
