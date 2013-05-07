
package com.continuent.tungsten.common.cluster.resource;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.net.MalformedURLException;

import org.junit.Test;

public class TestDataSource
{

    @Test
    public void testExtractPortFromJDBCUrl()
    {
        try
        {
            // verify default port
            assertEquals(3306,
                    DataSource.extractPortFromJDBCUrl("jdbc:mysql://localhost"));
            assertEquals(
                    5432,
                    DataSource
                            .extractPortFromJDBCUrl("jdbc:postgresql://localhost"));
            // port specified
            assertEquals(
                    3307,
                    DataSource
                            .extractPortFromJDBCUrl("jdbc:mysql://localhost:3307"));
            assertEquals(
                    5433,
                    DataSource
                            .extractPortFromJDBCUrl("jdbc:postgresql://localhost:5433"));
            assertEquals(
                    3307,
                    DataSource
                            .extractPortFromJDBCUrl("jdbc:mysql://localhost:3307/db"));
            assertEquals(
                    5433,
                    DataSource
                            .extractPortFromJDBCUrl("jdbc:postgresql://localhost:5433/db"));
            assertEquals(
                    3307,
                    DataSource
                            .extractPortFromJDBCUrl("jdbc:mysql://localhost:3307/db&password=fq:"));
            assertEquals(
                    5433,
                    DataSource
                            .extractPortFromJDBCUrl("jdbc:postgresql://localhost:5433/db&password=fq:"));

        }
        catch (MalformedURLException e)
        {
            fail("Got unexpected exception" + e);
        }
        // negative tests
        try
        {
            DataSource.extractPortFromJDBCUrl("jdbc:whatsoever://localhost");
            fail("Didn't get expected excpetion");
        }
        catch (MalformedURLException expected)
        {
        }
        // negative tests
        try
        {
            DataSource.extractPortFromJDBCUrl("jdbc:mysql://localhost:asdf");
            fail("Didn't get expected excpetion");
        }
        catch (MalformedURLException expected)
        {
        }
    }
}
