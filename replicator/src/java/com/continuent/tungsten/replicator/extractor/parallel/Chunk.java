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

    public Table getTable();

    /**
     * Returns the columns value.
     * 
     * @return Returns the columns.
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
     * Returns the nbBlocks value.
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

    public String getQuery(Database connection, String eventId);

    public Object[] getFromValues();

    public Object[] getToValues();
}
