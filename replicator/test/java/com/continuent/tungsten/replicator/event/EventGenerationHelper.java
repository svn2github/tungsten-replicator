/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2010 Continuent Inc.
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

import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;

import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnSpec;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnVal;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;

/**
 * Implements a simple helper to generate events for testing.
 */
public class EventGenerationHelper
{
    /**
     * Creates an event from a query.
     * 
     * @param seqno Sequence number
     * @param defaultSchema Default schema
     * @param query
     * @return A properly constructed event.
     */
    public ReplDBMSEvent eventFromStatement(long seqno, String defaultSchema,
            String query, int fragNo, boolean lastFrag)
    {
        Timestamp ts = new Timestamp(System.currentTimeMillis());
        ArrayList<DBMSData> t = new ArrayList<DBMSData>();
        t.add(new StatementData(query, ts.getTime(), defaultSchema));
        DBMSEvent dbmsEvent = new DBMSEvent(new Long(seqno).toString(), null,
                t, lastFrag, new Timestamp(System.currentTimeMillis()));
        ReplDBMSEvent replDbmsEvent = new ReplDBMSEvent(seqno, (short) fragNo,
                lastFrag, "NONE", 0, ts, dbmsEvent);
        return replDbmsEvent;
    }

    /**
     * Utility method to generate a non-fragment event from a statement.
     */
    public ReplDBMSEvent eventFromStatement(long seqno, String defaultSchema,
            String query)
    {
        return eventFromStatement(seqno, defaultSchema, query, 0, true);
    }

    /**
     * Creates a transaction event from a row insert.
     * 
     * @param seqno Sequence number
     * @param schema Schema name
     * @param table Table name
     * @param names Column names
     * @param values Value columns
     * @param fragNo Fragment number within transaction
     * @param lastFrag If true, last fragment in the transaction
     * @return A fully formed event containing a single row change
     */
    public ReplDBMSEvent eventFromRowInsert(long seqno, String schema,
            String table, String[] names, Object[] values, int fragNo,
            boolean lastFrag)
    {
        // Create row change data. This will contain a set of updates.
        OneRowChange rowChange = generateRowChange(schema, table,
                RowChangeData.ActionType.INSERT);

        // Add specifications and values for keys. This is the before value.
        rowChange.setKeySpec(generateSpec(rowChange, names));
        rowChange.setColumnValues(generateValues(rowChange, values));

        // Wrap the row change in a change set.
        RowChangeData rowChangeData = new RowChangeData();
        rowChangeData.appendOneRowChange(rowChange);

        // Add the change set to the event array and generate a DBMS
        // transaction.
        ArrayList<DBMSData> data = new ArrayList<DBMSData>();
        data.add(rowChangeData);

        Timestamp ts = new Timestamp(System.currentTimeMillis());
        DBMSEvent dbmsEvent = new DBMSEvent(new Long(seqno).toString(), null,
                data, lastFrag, new Timestamp(System.currentTimeMillis()));
        ReplDBMSEvent replDbmsEvent = new ReplDBMSEvent(seqno, (short) fragNo,
                lastFrag, "NONE", 0, ts, dbmsEvent);
        return replDbmsEvent;
    }

    // Generate table change header.
    private OneRowChange generateRowChange(String schema, String table,
            RowChangeData.ActionType action)
    {
        // Create table change header.
        OneRowChange oneRowChange = new OneRowChange();
        oneRowChange.setSchemaName(schema);
        oneRowChange.setTableName(table);
        oneRowChange.setTableId(1);
        oneRowChange.setAction(action);
        return oneRowChange;
    }

    // Generate specifications for a row event. This currently handles strings
    // only.
    private ArrayList<ColumnSpec> generateSpec(OneRowChange oneRowChange,
            String[] names)
    {
        // Generate for array of column specification data.
        ArrayList<ColumnSpec> specArray = new ArrayList<ColumnSpec>(
                names.length);

        // Iterate through the name array, adding a specification for each.
        for (int i = 0; i < specArray.size(); i++)
        {
            ColumnSpec colSpec = oneRowChange.new ColumnSpec();
            colSpec.setIndex(1);
            colSpec.setName("c1");
            colSpec.setType(Types.VARCHAR);
            specArray.add(colSpec);
        }

        return specArray;
    }

    // Generate values for a row event.
    private ArrayList<ArrayList<ColumnVal>> generateValues(
            OneRowChange oneRowChange, Object[] values)
    {
        // Create the array to hold the values.
        ArrayList<ArrayList<ColumnVal>> valueList = new ArrayList<ArrayList<ColumnVal>>(
                1);
        ArrayList<ColumnVal> valueColumns = new ArrayList<ColumnVal>(
                values.length);

        // Iterate through the value columns and add each value.
        for (int i = 0; i < valueColumns.size(); i++)
        {
            ColumnVal cv1 = oneRowChange.new ColumnVal();
            cv1.setValue(values[i].toString());
            valueColumns.add(cv1);
        }

        // Return the resulting list.
        return valueList;
    }
}