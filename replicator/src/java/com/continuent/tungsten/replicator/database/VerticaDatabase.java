/**
 * Tungsten: An Application Server for uni/cluster.
 * Copyright (C) 2007-2011 Continuent Inc.
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

package com.continuent.tungsten.replicator.database;

import java.io.BufferedWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

import com.continuent.tungsten.commons.csv.CsvWriter;
import com.continuent.tungsten.commons.csv.NullPolicy;

/**
 * Implements DBMS-specific operations for Vertica.
 * 
 * @author <a href="mailto:linas.virbalas@continuent.com">Linas Virbalas</a>
 */
public class VerticaDatabase extends PostgreSQLDatabase
{
    private static Logger logger = Logger.getLogger(VerticaDatabase.class);

    public VerticaDatabase() throws SQLException
    {
        dbms = DBMS.VERTICA;
        // Hard code the version 4.x driver so it gets loaded.
        dbDriver = "com.vertica.Driver";
        // Check to see if the version 4.x driver is present.  If not, use
        // the 5.x driver name. 
        try
        {
            logger.debug("Checking for default driver: " + dbDriver);
            Class.forName(dbDriver);
        }
        catch (Exception e)
        {
            dbDriver = "com.vertica.jdbc.Driver";
            logger.debug("Unable to load Vertica 4.x JDBC driver; will default to Version 5.x name: " + dbDriver);
        }

    }

    /**
     * Overload connect method to issue call to ensure default projections are
     * enabled.
     * 
     * @see com.continuent.tungsten.replicator.database.Database#connect(boolean)
     */
    public synchronized void connect(boolean binlog) throws SQLException
    {
        // Connect first.
        super.connect(binlog);

        // Issue call to define projections.
        if (connected)
            execute("SELECT implement_temp_design('')");
    }

    /**
     * Ensure projection is created for new table. (Otherwise metadata
     * operations may fail if we try to select off a new table.)
     * 
     * @see com.continuent.tungsten.replicator.database.Database#createTable(com.continuent.tungsten.replicator.database.Table,
     *      boolean)
     */
    public void createTable(Table t, boolean replace) throws SQLException
    {
        // Create table.
        super.createTable(t, replace);

        // Ensure projection is created.
        execute("SELECT implement_temp_design('')");
    }

    /**
     * Checks whether the given table exists in the currently connected database
     * using Vertica-specific v_catalog.tables view.
     * 
     * @return true, if table exists, false, if not.
     */
    protected boolean tableExists(Table t) throws SQLException
    {
        String sql = String
                .format("SELECT * FROM v_catalog.tables WHERE table_schema='%s' AND table_name='%s'",
                        t.getSchema(), t.getName());
        Statement stmt = dbConn.createStatement();
        try
        {
            ResultSet rs = stmt.executeQuery(sql);
            return rs.next();
        }
        finally
        {
            if (stmt != null)
            {
                try
                {
                    stmt.close();
                }
                catch (SQLException e)
                {
                }
            }
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#getCsvWriter(java.io.BufferedWriter)
     */
    public CsvWriter getCsvWriter(BufferedWriter writer)
    {
        CsvWriter csv = new CsvWriter(writer);
        csv.setQuoteChar('"');
        csv.setQuoted(true);
        csv.setNullPolicy(NullPolicy.skip);
        csv.setEscapedChars("\\");
        csv.setSuppressedChars("\n");
        csv.setEscapeChar('\\');
        csv.setWriteHeaders(false);
        return csv;
    }
}
