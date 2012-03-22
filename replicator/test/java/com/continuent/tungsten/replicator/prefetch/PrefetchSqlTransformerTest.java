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

import junit.framework.TestCase;

/**
 * Tests prefetch transformation.
 */
public class PrefetchSqlTransformerTest extends TestCase
{
    /**
     * Verify we add limit if none exists and the limit is greater than 0.
     */
    public void testAddLimit() throws Exception
    {
        PrefetchSqlTransformer pst = new PrefetchSqlTransformer();

        String[] queries = {"select * from foo",
                "SELECT count(*) FROM foo ORDER by id ascending"};

        for (String q1 : queries)
        {
            String q2 = pst.addLimitToQuery(q1, 1);
            assertTrue("Length: " + q1, q2.length() > q1.length());
            assertTrue("Contents: " + q1, q2.toLowerCase().contains("limit 1"));

            String q3 = pst.addLimitToQuery(q1, 0);
            assertEquals("limit 0: " + q1, q1, q3);
        }
    }

    /**
     * Verify we do not add a limit if it already anywhere in the query.
     */
    public void testAddLimitWhenExists() throws Exception
    {
        PrefetchSqlTransformer pst = new PrefetchSqlTransformer();

        String[] queries = {"select * from foo limit 25",
                "select * from foo LIMIT 1",
                "SELECT count(*) FROM mylimit ORDER by id ascending"};

        for (String q1 : queries)
        {
            String q2 = pst.addLimitToQuery(q1, 25);
            assertEquals("No limited added: " + q1, q1, q2);
        }
    }
}