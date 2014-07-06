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

package com.continuent.tungsten.replicator.filter;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.event.EventGenerationHelper;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplOptionParams;

/**
 * This class implements a test of the SchemaChangeFilter class. It checks to
 * ensure schema change and truncate statements generate expected annotations on
 * statements and events.
 */
public class SchemaChangeFilterTest extends TestCase
{
    private FilterVerificationHelper filterHelper = new FilterVerificationHelper();
    private EventGenerationHelper    eventHelper  = new EventGenerationHelper();

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
     * Verify that the filter does not add any annotations if the statement is
     * not a schema change or truncate.
     */
    public void testUnspecifiedProperties() throws Exception
    {
        SchemaChangeFilter scf = new SchemaChangeFilter();
        filterHelper.setFilter(scf);
        ReplDBMSEvent e = filter(filterHelper, 0, "foo",
                "insert into bar(val) values(1)");
        assertEventPropertyNull(e, "schema_change");
        assertEventPropertyNull(e, "truncate");
        filterHelper.done();
    }

    /**
     * Verify that the filter adds annotations for table schema changes.
     */
    public void testSchemaChange() throws Exception
    {
        String[] changes = {"create table foo (id int primary key)",
                "DROP   TABLE foo",
                "alter table foo add column data1 varchar(30)",
                "alter table foo drop column data1"};
        SchemaChangeFilter scf = new SchemaChangeFilter();
        filterHelper.setFilter(scf);

        // Test changes.
        for (String change : changes)
        {
            ReplDBMSEvent e = filter(filterHelper, 0, "foo", change);
            assertEventProperty(e, "schema_change");
            assertEventPropertyNull(e, "truncate");

            assertStatementProperty(e, ReplOptionParams.OPERATION_NAME);
            assertStatementProperty(e, ReplOptionParams.SCHEMA_NAME);
            assertStatementProperty(e, ReplOptionParams.TABLE_NAME);
        }
        filterHelper.done();
    }

    /**
     * Verify that the filter adds annotations for table truncate commands.
     */
    public void testTruncate() throws Exception
    {
        SchemaChangeFilter scf = new SchemaChangeFilter();
        filterHelper.setFilter(scf);

        ReplDBMSEvent e = filter(filterHelper, 0, "foo", "truncate bar.foo");
        assertEventPropertyNull(e, "schema_change");
        assertEventProperty(e, "truncate");

        assertStatementProperty(e, ReplOptionParams.OPERATION_NAME);
        assertStatementProperty(e, ReplOptionParams.SCHEMA_NAME);
        assertStatementProperty(e, ReplOptionParams.TABLE_NAME);
        filterHelper.done();
    }

    /**
     * Ensures that a particular event property is set.
     */
    private void assertEventProperty(ReplDBMSEvent event, String name)
    {
        String value = event.getDBMSEvent().getMetadataOptionValue(name);
        DBMSData rawData = event.getData().get(0);
        String query = ((StatementData) rawData).getQuery();
        Assert.assertNotNull("Expected property to be set: query=" + query
                + " name=" + name, value);
    }

    /**
     * Ensures that a particular event property is unset.
     */
    private void assertEventPropertyNull(ReplDBMSEvent event, String name)
    {
        String value = event.getDBMSEvent().getMetadataOptionValue(name);
        DBMSData rawData = event.getData().get(0);
        String query = ((StatementData) rawData).getQuery();
        Assert.assertNull("Expected property to be unset: query=" + query
                + " name=" + name, value);
    }

    /**
     * Ensures property for a particular statement is set.
     */
    private void assertStatementProperty(ReplDBMSEvent event, String name)
    {
        String value = event.getData().get(0).getOption(name);
        Assert.assertNotNull("Expected statement property to be set: " + name,
                value);
    }

    /**
     * Filter a single event.
     */
    private ReplDBMSEvent filter(FilterVerificationHelper filterHelper,
            ReplDBMSEvent e) throws ReplicatorException, InterruptedException
    {
        ReplDBMSEvent e2 = filterHelper.filter(e);
        return e2;
    }

    /**
     * Filter a single event built up from seqno, schema, and a query.
     */
    private ReplDBMSEvent filter(FilterVerificationHelper filterHelper,
            long seqno, String defaultSchema, String query)
            throws ReplicatorException, InterruptedException
    {
        ReplDBMSEvent e = eventHelper.eventFromStatement(seqno, defaultSchema,
                query);
        return filter(filterHelper, e);
    }
}