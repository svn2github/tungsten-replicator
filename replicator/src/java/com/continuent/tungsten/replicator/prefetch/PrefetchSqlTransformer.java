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

/**
 * Contains utility methods to perform standard transformations to aid prefetch.
 * This logic is encapsulated in a separate class that is easy to unit test.
 */
public class PrefetchSqlTransformer
{
    /**
     * Adds a LIMIT clause to the query if none is present already *and* the
     * limit is greater than 0.
     * 
     * @param query Query which should be adorned
     * @param limit Number of rows to limit; must be greater than 0
     * @return Query with limit clause
     */
    public String addLimitToQuery(String query, int limit)
    {
        // Check preconditions, then add the clause.
        if (limit <= 0)
            return query;
        else if (query.toLowerCase().contains("limit"))
            return query;
        else
        {
            String limitClause = String.format(" LIMIT %d", limit);
            return query + limitClause;
        }
    }
}