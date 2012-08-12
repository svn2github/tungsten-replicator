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
 * Initial developer(s): Linas Virbalas
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.filter;

import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;

import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnSpec;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnVal;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;

/**
 * This class tests OptimizeUpdatesFilter and corner cases.
 * 
 * @author <a href="mailto:linas.virbalas@continuent.com">Linas Virbalas</a>
 */
public class OptimizeUpdatesFilterTest extends TestCase
{
//    private static Logger  logger = Logger.getLogger(OptimizeUpdatesFilterTest.class);

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
     * Test UPDATE which has some columns changed and some - static.
     */
    public void testUPDATEWith1of3ColChange() throws Exception
    {
        OptimizeUpdatesFilter filter = new OptimizeUpdatesFilter();
        
        // Create table change header.
        OneRowChange oneRowChange = generateRowChange("foo", "bar",
                RowChangeData.ActionType.UPDATE);

        // Add specifications and values for columns. This is the after value.
        oneRowChange.setColumnSpec(generateSpec(oneRowChange));
        oneRowChange.setColumnValues(generateValues(oneRowChange, 333, "two", "const"));

        // Add specifications and values for keys. This is the before value.
        oneRowChange.setKeySpec(generateSpec(oneRowChange));
        oneRowChange.setKeyValues(generateValues(oneRowChange, 333, "one", "const"));
        
        // Two identical events for comparison.
        ReplDBMSEvent event = generateReplDBMSEvent(oneRowChange);
        
        // Filter and verify results!
        filter.filter(event);
        
        RowChangeData rdata = (RowChangeData)event.getDBMSEvent().getData().get(0);
        OneRowChange orc = rdata.getRowChanges().get(0);
        
        Assert.assertEquals("Two column specs were removed", 1, orc.getColumnSpec().size());
        Assert.assertEquals("Two column values were removed", 1, orc.getColumnValues().get(0).size());
        Assert.assertEquals("All key specs are in place", 3, orc.getKeySpec().size());
        Assert.assertEquals("All key values are in place", 3, orc.getKeyValues().get(0).size());
    }

    /**
     * Issue 355 - Oracle UPDATEs that change no columns lead to
     * OptimizeUpdatesFilter removing too much.
     */
    public void testUPDATEWithoutChanges() throws Exception
    {
        OptimizeUpdatesFilter filter = new OptimizeUpdatesFilter();
        
        // Create table change header.
        OneRowChange oneRowChange = generateRowChange("foo", "bar",
                RowChangeData.ActionType.UPDATE);

        // Add specifications and values for columns. This is the after value.
        oneRowChange.setColumnSpec(generateSpec(oneRowChange));
        oneRowChange.setColumnValues(generateValues(oneRowChange, 333, "CONST", "const"));

        // Add specifications and values for keys. This is the before value.
        oneRowChange.setKeySpec(generateSpec(oneRowChange));
        oneRowChange.setKeyValues(generateValues(oneRowChange, 333, "CONST", "const"));
        
        // Two identical events for comparison.
        ReplDBMSEvent event = generateReplDBMSEvent(oneRowChange);
        
        // Filter and verify results!
        filter.filter(event);
        
        RowChangeData rdata = (RowChangeData)event.getDBMSEvent().getData().get(0);
        OneRowChange orc = rdata.getRowChanges().get(0);
        
        Assert.assertEquals("All column specs are in place", 3, orc.getColumnSpec().size());
        Assert.assertEquals("All column values are in place", 3, orc.getColumnValues().get(0).size());
        Assert.assertEquals("All key specs are in place", 3, orc.getKeySpec().size());
        Assert.assertEquals("All key values are in place", 3, orc.getKeyValues().get(0).size());
    }
    
    
    /**
     * Generates ReplDBMSEvent with a single OneRowChange.
     */
    private ReplDBMSEvent generateReplDBMSEvent(OneRowChange oneRowChange)
    {
        long seqno = 1;
        
        RowChangeData rowChangeData = new RowChangeData();
        rowChangeData.appendOneRowChange(oneRowChange);

        ArrayList<DBMSData> data = new ArrayList<DBMSData>();
        data.add(rowChangeData);
        
        DBMSEvent dbmsEvent = new DBMSEvent(new Long(seqno).toString(), null,
                data, true, new Timestamp(System.currentTimeMillis()));
        
        return new ReplDBMSEvent(seqno, dbmsEvent);
    }
    
    /**
     * Generate table change header.
     */
    private OneRowChange generateRowChange(String schema, String table,
            RowChangeData.ActionType action)
    {
        // Create table change header.
        OneRowChange oneRowChange = new OneRowChange();
        oneRowChange.setSchemaName("foo");
        oneRowChange.setTableName("bar");
        oneRowChange.setTableId(1);
        oneRowChange.setAction(action);
        return oneRowChange;
    }

    /**
     * Generate specifications.
     */
    private ArrayList<ColumnSpec> generateSpec(OneRowChange oneRowChange)
    {
        ArrayList<ColumnSpec> spec = new ArrayList<ColumnSpec>(3);
        
        ColumnSpec c1 = oneRowChange.new ColumnSpec();
        c1.setIndex(1);
        c1.setName("c1");
        c1.setType(Types.INTEGER);
        spec.add(c1);
        
        ColumnSpec c2 = oneRowChange.new ColumnSpec();
        c2.setIndex(2);
        c2.setName("c2");
        c2.setType(Types.CHAR);
        c2.setLength(10);
        spec.add(c2);
        
        ColumnSpec c3 = oneRowChange.new ColumnSpec();
        c3.setIndex(3);
        c3.setName("c3");
        c3.setType(Types.VARCHAR);
        c3.setLength(32);
        spec.add(c3);
        
        return spec;
    }
    
    /**
     * Generate values.
     */
    private ArrayList<ArrayList<ColumnVal>> generateValues(
            OneRowChange oneRowChange, int v1, String v2, String v3)
    {
        ArrayList<ArrayList<ColumnVal>> list = new ArrayList<ArrayList<ColumnVal>>(
                1);
        ArrayList<ColumnVal> values = new ArrayList<ColumnVal>(3);
        
        ColumnVal cv1 = oneRowChange.new ColumnVal();
        cv1.setValue(new Integer(v1));
        values.add(cv1);
        
        ColumnVal cv2 = oneRowChange.new ColumnVal();
        cv2.setValue(v2);
        values.add(cv2);
        
        ColumnVal cv3 = oneRowChange.new ColumnVal();
        cv3.setValue(v3);
        values.add(cv3);
        
        list.add(values);
        
        return list;
    }
}