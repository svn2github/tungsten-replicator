/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2013 Continuent Inc.
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

package com.continuent.tungsten.replicator.ddlscan;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.DatabaseFactory;
import com.continuent.tungsten.replicator.database.Table;
import com.continuent.tungsten.replicator.database.TableMatcher;

/**
 * Main DDLScan functionality is programmed here.
 * 
 * @author <a href="mailto:linas.virbalas@continuent.com">Linas Virbalas</a>
 * @version 1.0
 */
public class DDLScan
{
    private static Logger logger       = Logger.getLogger(DDLScan.class);

    private String        url          = null;
    private String        dbName       = null;
    private String        user         = null;
    private String        pass         = null;

    private String        templateFile = null;

    private Database      db           = null;
    private Connection    conn         = null;

    /**
     * Creates a new <code>DDLScan</code> object from provided JDBC URL
     * connection credentials and template file.
     * 
     * @param url JDBC URL connection string.
     * @param dbName Database/schema to connect to.
     * @throws Exception
     */
    public DDLScan(String url, String dbName, String user, String pass,
            String templateFile) throws ReplicatorException
    {
        this.url = url;
        this.user = user;
        this.pass = pass;
        this.dbName = dbName;

        this.templateFile = templateFile;
    }

    /**
     * Connect to the underlying database.
     * 
     * @throws ReplicatorException
     */
    public void prepare() throws ReplicatorException, InterruptedException,
            SQLException
    {
        db = DatabaseFactory.createDatabase(url, user, pass);
        db.connect();

        conn = db.getConnection();
    }

    /**
     * Scans and extracts metadata from the database of requested tables.
     * 
     * @param tables Regular expression enable list of tables to find or null
     *            for all tables.
     */
    public void scan(String tablesToFind) throws ReplicatorException,
            InterruptedException, SQLException
    {
        // Regular expression matcher for tables.
        TableMatcher tableMatcher = null;
        if (tablesToFind != null)
            tableMatcher = extractFilter(tablesToFind);

        // Iterate through all available tables in the database.
        ArrayList<Table> tables = db.getTables(dbName, true);

        for (Table table : tables)
        {
            if (tableMatcher != null
                    && tableMatcher.match(table.getSchema(), table.getName()))
            {
                // TODO: extract metadata.
            }
        }
    }

    /**
     * Prepares table regex matcher.
     * 
     * @param filter Regex enabled list of tables.
     */
    private TableMatcher extractFilter(String filter)
    {
        // If empty, we do nothing.
        if (filter == null || filter.length() == 0)
            return null;

        TableMatcher tableMatcher = new TableMatcher();
        tableMatcher.prepare(filter);
        return tableMatcher;
    }

    /**
     * Generate output by applying extracted metadata to a template.
     * 
     * @param templateFile Path to Velocity template.
     */
    public String generate(String templateFile)
    {
        String out = null;

        return out;
    }

    /**
     * Disconnect from the THL database.
     */
    public void release()
    {
        if (db != null)
        {
            // This also closes connection.
            db.close();
            db = null;
        }
    }

}
