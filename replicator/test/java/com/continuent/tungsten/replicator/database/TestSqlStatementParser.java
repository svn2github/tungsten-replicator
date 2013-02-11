/**
 * Tungsten: An Application Server for uni/cluster.
 * Copyright (C) 2013 Continuent Inc.
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

package com.continuent.tungsten.replicator.database;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests SQL parsing capabilities for different DBMS types. (Currently parsing
 * is only supported for MySQL.)
 */
public class TestSqlStatementParser
{
    /**
     * Verify that basic wiring works for MySQL.
     */
    @Test
    public void testBasicParsing() throws Exception
    {
        String cmd = "create database foo";
        verifyCreateDatabase(cmd, Database.MYSQL);
    }

    /**
     * Verify all known DBMS types as well as the infamous null type.
     */
    @Test
    public void testAllDbmsTypes() throws Exception
    {
        String[] dbmsTypes = {Database.MYSQL, Database.ORACLE,
                Database.POSTGRESQL, Database.UNKNOWN, null};
        String cmd = "create database foo";
        for (String dbmsType : dbmsTypes)
        {
            verifyCreateDatabase(cmd, dbmsType);
        }
    }

    /**
     * Verify that we can do a large number of parses in a short period of time.
     */
    @Test
    public void testManyParsingCalls() throws Exception
    {
        String cmd = "create database foo";
        long startMillis = System.currentTimeMillis();
        for (int i = 0; i < 100000; i++)
        {
            verifyCreateDatabase(cmd, Database.MYSQL);
        }
        long duration = System.currentTimeMillis() - startMillis;
        if ((duration / 1000) > 5)
            throw new Exception("Parsing calls took way too long");
    }

    /**
     * Run a simple parsing test.
     */
    private void verifyCreateDatabase(String cmd, String dbmsType)
    {
        SqlStatementParser p = SqlStatementParser.getParser();
        SqlOperation op = p.parse(cmd, dbmsType);
        Assert.assertEquals("Found object type: " + dbmsType,
                SqlOperation.SCHEMA, op.getObjectType());
        Assert.assertEquals("Found operation: " + dbmsType,
                SqlOperation.CREATE, op.getOperation());
        Assert.assertEquals("Found database: " + dbmsType, "foo",
                op.getSchema());
        Assert.assertNull("No table: " + dbmsType, op.getName());
        Assert.assertTrue("Is autocommit: " + dbmsType, op.isAutoCommit());
    }
}