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

import java.io.IOException;
import java.io.Writer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Hashtable;

import org.apache.velocity.VelocityContext;
import org.apache.velocity.Template;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.exception.ResourceNotFoundException;
import org.apache.velocity.exception.ParseErrorException;
import org.apache.velocity.exception.MethodInvocationException;
import org.apache.velocity.runtime.RuntimeConstants;
import org.apache.velocity.runtime.log.Log4JLogChute;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.database.Column;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.DatabaseFactory;
import com.continuent.tungsten.replicator.database.OracleDatabase;
import com.continuent.tungsten.replicator.database.Table;
import com.continuent.tungsten.replicator.database.TableMatcher;
import com.continuent.tungsten.replicator.filter.EnumToStringFilter;
import com.continuent.tungsten.replicator.filter.RenameDefinitions;

/**
 * Main DDLScan functionality is programmed here.
 * 
 * @author <a href="mailto:linas.virbalas@continuent.com">Linas Virbalas</a>
 * @version 1.0
 */
public class DDLScan
{
    private String            url                 = null;
    private String            dbName              = null;
    private String            user                = null;
    private String            pass                = null;

    private Template          template            = null;

    private Database          db                  = null;

    private ArrayList<String> reservedWordsOracle = null;

    VelocityEngine            velocity            = null;
    RenameDefinitions         renameDefinitions   = null;

    /**
     * Creates a new <code>DDLScan</code> object from provided JDBC URL
     * connection credentials and template file.
     * 
     * @param url JDBC URL connection string.
     * @param dbName Database/schema to connect to.
     * @throws Exception
     */
    public DDLScan(String url, String dbName, String user, String pass)
            throws ReplicatorException
    {
        this.url = url;
        this.user = user;
        this.pass = pass;
        this.dbName = dbName;
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
        
        // Prepare reserved words lists.
        OracleDatabase oracle = new OracleDatabase();
        reservedWordsOracle = oracle.getReservedWords();

        // Configure and initialize Velocity engine. Using ourselves as a
        // logger.
        velocity = new VelocityEngine();
        velocity.setProperty(RuntimeConstants.RUNTIME_LOG_LOGSYSTEM_CLASS,
                "org.apache.velocity.runtime.log.Log4JLogChute");
        velocity.setProperty(Log4JLogChute.RUNTIME_LOG_LOG4J_LOGGER,
                DDLScan.class.toString());
        velocity.setProperty(RuntimeConstants.FILE_RESOURCE_LOADER_PATH,
                ".,../samples/extensions/velocity");
        velocity.init();
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
     * Compiles the given Velocity template file.
     */
    public void parseTemplate(String templateFile)
            throws ReplicatorException
    {
        try
        {
            // Parse the template.
            template = velocity.getTemplate(templateFile);
        }
        catch (ResourceNotFoundException rnfe)
        {
            throw new ReplicatorException("Couldn't open the template", rnfe);
        }
        catch (ParseErrorException pee)
        {
            throw new ReplicatorException("Problem parsing the template", pee);
        }
    }
    
    /**
     * Tries to load rename definitions file. It will be used for all subsequent
     * scan calls.
     * 
     * @throws ReplicatorException On parsing or CSV format errors.
     * @throws IOException If file cannot be read.
     * @see #resetRenameDefinitions()
     * @see #scan(String, Writer)
     */
    public void parseRenameDefinitions(String definitionsFile)
            throws ReplicatorException, IOException
    {
        renameDefinitions = new RenameDefinitions(definitionsFile);
        renameDefinitions.parseFile();
    }
    
    /**
     * Stop using rename definitions file for future scan(...) calls.
     * 
     * @see #parseRenameDefinitions(String)
     * @see #scan(String, Writer)
     */
    public void resetRenameDefinitions()
    {
        renameDefinitions = null;
    }

    /**
     * Scans and extracts metadata from the database of requested tables. Calls
     * merge(...) against each found table.
     * 
     * @param tablesToFind Regular expression enable list of tables to find or
     *            null for all tables.
     * @param templateOptions Options (option->value) to pass to the template.
     * @param writer Writer object to use for appending rendered template. Make
     *            sure to initialize it before and flush/close it after
     *            manually.
     * @return Rendered template data.
     */
    public String scan(String tablesToFind,
            Hashtable<String, String> templateOptions, Writer writer)
            throws ReplicatorException, InterruptedException, SQLException,
            IOException
    {
        // Regular expression matcher for tables.
        TableMatcher tableMatcher = null;
        if (tablesToFind != null)
            tableMatcher = extractFilter(tablesToFind);

        // Retrieve all tables available.
        ArrayList<Table> tables = db.getTables(dbName, true);

        // Make a context object and populate with the data. This is where
        // the Velocity engine gets the data to resolve the references in
        // the template.
        VelocityContext context = new VelocityContext();

        // User options passed to the template.
        for (String option : templateOptions.keySet())
        {
            context.put(option, templateOptions.get(option));
        }

        // Source connection details.
        context.put("dbName", dbName);
        context.put("user", user);
        context.put("url", url);

        // Database object.
        context.put("db", db);

        // RenameDefinitions object used (if any).
        context.put("renameDefinitions", renameDefinitions);

        // Some handy utilities.
        context.put("enum", EnumToStringFilter.class);
        context.put("date", new java.util.Date()); // Current time.
        context.put("reservedWordsOracle", reservedWordsOracle);

        // Iterate through all available tables in the database.
        for (Table table : tables)
        {
            // Is this table requested by the user?
            if (tableMatcher == null
                    || tableMatcher.match(table.getSchema(), table.getName()))
            {
                // If requested, do the renaming.
                rename(table);

                // Velocity merge.
                merge(context, table, writer);
            }
        }

        return writer.toString();
    }

    /**
     * If renameDefinitions object is prepared, does the lookup and renaming of
     * schema, table and columns. Nothing is done if renameDefinitions is null.
     * 
     * @see #parseRenameDefinitions(String)
     */
    private void rename(Table table)
    {
        if (renameDefinitions != null)
        {
            // Rename columns.
            for (Column col : table.getAllColumns())
            {
                String newColName = renameDefinitions.getNewColumnName(
                        table.getSchema(), table.getName(), col.getName());
                if (newColName != null)
                    col.setName(newColName);
            }

            // Get new table name if there's a request.
            String newTableName = renameDefinitions.getNewTableName(
                    table.getSchema(), table.getName());

            // Get new schema name if there's a request.
            String newSchemaName = renameDefinitions.getNewSchemaName(
                    table.getSchema(), table.getName());

            // Finally, do the actual renaming of schema and table.
            if (newTableName != null)
                table.setTable(newTableName);
            if (newSchemaName != null)
                table.setSchema(newSchemaName);
        }
    }

    /**
     * Generate output by merging extracted metadata to a template.
     * 
     * @param context Context is shared each time, only table is changed.
     * @param templateFile Path to Velocity template.
     * @param writer Initialized Writer object to append output to.
     */
    private void merge(VelocityContext context, Table table, Writer writer)
            throws ReplicatorException
    {
        try
        {
            // Actual table with columns, keys, etc.
            context.put("table", table);

            // Now have the template engine process the template using the data
            // placed into the context. Think of it as a 'merge' of the template
            // and the data to produce the output stream.
            if (template != null)
                template.merge(context, writer);
        }
        catch (MethodInvocationException mie)
        {
            throw new ReplicatorException(
                    "Something invoked in the template caused problem", mie);
        }
        catch (Exception e)
        {
            throw new ReplicatorException(e);
        }
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
