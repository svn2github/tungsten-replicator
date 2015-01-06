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
 * Initial developer(s): Robert Hodges
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.event;

import java.util.ArrayList;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.RowChangeData;

/**
 * Implements a test of row change functionality including functions to check
 * presence of types.
 */
public class RowChangeMetadataTest
{
    private EventGenerationHelper eventHelper = new EventGenerationHelper();

    /**
     * Setup.
     * 
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
    }

    /**
     * Teardown.
     * 
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
    }

    /**
     * Verify that row change metadata correctly counts the number of types
     * present in an insert row change. 
     */
    @Test
    public void testRowChangeTypesCountInsert() throws Exception
    {
        // Create a single insert row change event.
        String names[] = new String[2];
        Integer values[] = new Integer[2];
        for (int i = 0; i < names.length; i++)
        {
            names[i] = "data-" + i;
            values[i] = i;
        }
        ReplDBMSEvent insertEvent = eventHelper.eventFromRowInsert(0, "schema",
                "table", names, values, 0, true);

        // Check for presence of data types.
        ArrayList<DBMSData> eventData = insertEvent.getData();
        RowChangeData rowChangeData = (RowChangeData) eventData.get(0);
        OneRowChange rowChange = rowChangeData.getRowChanges().get(0);

        // We expect to find a two VARCHAR values, since event helper types
        // things as strings.
        Assert.assertEquals("Expect two varchars", 2,
                rowChange.typeCount(java.sql.Types.VARCHAR));
        Assert.assertTrue("Expect varchar to be present",
                rowChange.hasType(java.sql.Types.VARCHAR));

        // We do not expect to find any varbinary values.
        Assert.assertEquals("Varbinary count should be 0", 0,
                rowChange.typeCount(java.sql.Types.VARBINARY));
        Assert.assertFalse("Varbinary should not be present",
                rowChange.hasType(java.sql.Types.VARBINARY));
    }
    /**
     * Verify that row change metadata correctly counts the number of types
     * present in a delete. 
     */
    @Test
    public void testRowChangeTypesCountDelete() throws Exception
    {
        // Create a single insert row change event.
        String names[] = new String[1];
        Integer values[] = new Integer[1];
        for (int i = 0; i < names.length; i++)
        {
            names[i] = "data-" + i;
            values[i] = i;
        }
        ReplDBMSEvent insertEvent = eventHelper.eventFromRowDelete(0, "schema",
                "table", names, values, 0, true);

        // Check for presence of data types.
        ArrayList<DBMSData> eventData = insertEvent.getData();
        RowChangeData rowChangeData = (RowChangeData) eventData.get(0);
        OneRowChange rowChange = rowChangeData.getRowChanges().get(0);

        // We expect to find a two VARCHAR values, since event helper types
        // things as strings.
        Assert.assertEquals("Expect one varchar", 1,
                rowChange.typeCount(java.sql.Types.VARCHAR));
        Assert.assertTrue("Expect varchar to be present",
                rowChange.hasType(java.sql.Types.VARCHAR));

        // We do not expect to find any set values.
        Assert.assertEquals("Varbinary count should be 0", 0,
                rowChange.typeCount(java.sql.Types.VARBINARY));
        Assert.assertFalse("Varbinary should not be present",
                rowChange.hasType(java.sql.Types.VARBINARY));
    }
}