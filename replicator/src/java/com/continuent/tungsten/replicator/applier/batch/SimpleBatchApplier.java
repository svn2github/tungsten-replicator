/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2011-13 Continuent Inc.
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
 * Contributor(s): Linas Virbalas
 */

package com.continuent.tungsten.replicator.applier.batch;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.regex.Pattern;

import javax.sql.rowset.serial.SerialException;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.csv.CsvException;
import com.continuent.tungsten.common.csv.CsvWriter;
import com.continuent.tungsten.common.exec.ProcessExecutor;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.applier.RawApplier;
import com.continuent.tungsten.replicator.consistency.ConsistencyException;
import com.continuent.tungsten.replicator.consistency.ConsistencyTable;
import com.continuent.tungsten.replicator.database.Column;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.DatabaseFactory;
import com.continuent.tungsten.replicator.database.Key;
import com.continuent.tungsten.replicator.database.SqlScriptGenerator;
import com.continuent.tungsten.replicator.database.Table;
import com.continuent.tungsten.replicator.database.TableMetadataCache;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.LoadDataFileFragment;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnSpec;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnVal;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.dbms.RowChangeData.ActionType;
import com.continuent.tungsten.replicator.dbms.RowIdData;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.event.ReplOptionParams;
import com.continuent.tungsten.replicator.extractor.mysql.SerialBlob;
import com.continuent.tungsten.replicator.heartbeat.HeartbeatTable;
import com.continuent.tungsten.replicator.plugin.PluginContext;
import com.continuent.tungsten.replicator.thl.CommitSeqnoTable;

/**
 * Implements an applier that bulk loads data into a SQL database via CSV files.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class SimpleBatchApplier implements RawApplier
{
    private static Logger               logger               = Logger.getLogger(SimpleBatchApplier.class);

    /**
     * Denotes an insert operation.
     */
    public static String                INSERT               = "I";

    /**
     * Denotes a delete operation.
     */
    public static String                DELETE               = "D";

    // Task management information.
    private int                         taskId;

    // Properties.
    protected String                    driver;
    protected String                    url;
    protected String                    user;
    protected String                    password;
    protected String                    stageDirectory;
    protected String                    startupScript;
    protected String                    stageMergeScript;
    protected String                    stageSchemaPrefix;
    protected String                    stageTablePrefix;
    protected String                    stageColumnPrefix    = "tungsten_";
    protected String                    stagePkeyColumn;
    protected boolean                   cleanUpFiles         = true;
    protected String                    charset              = "UTF-8";
    protected String                    timezone             = "GMT-0:00";
    protected LoadMismatch              onLoadMismatch       = LoadMismatch.fail;
    protected boolean                   showCommands;

    // Load file directory for this task.
    private File                        stageDir;

    // Character set for writing CSV files.
    private Charset                     outputCharset;

    // Open CVS files in current transaction.
    private Map<String, CsvInfo>        openCsvFiles         = new TreeMap<String, CsvInfo>();

    // Cached merge commands.
    private BatchScript                 mergeScript          = new BatchScript();

    // Latest event.
    private ReplDBMSHeader              latestHeader;

    // Table metadata for base tables.
    private TableMetadataCache          fullMetadataCache;

    // DBMS connection information.
    protected String                    metadataSchema       = null;
    protected String                    consistencyTable     = null;
    protected String                    consistencySelect    = null;
    protected Database                  conn                 = null;
    protected Statement                 statement            = null;
    protected Pattern                   ignoreSessionPattern = null;

    // Catalog tables.
    protected CommitSeqnoTable          commitSeqnoTable     = null;
    protected HeartbeatTable            heartbeatTable       = null;

    // Data formatter.
    protected volatile SimpleDateFormat dateFormatter;

    public void setDriver(String driver)
    {
        this.driver = driver;
    }

    public void setUrl(String url)
    {
        this.url = url;
    }

    public void setUser(String user)
    {
        this.user = user;
    }

    public void setPassword(String password)
    {
        this.password = password;
    }

    /** Set the name of the connect script. */
    public void setStartupScript(String startupScript)
    {
        this.startupScript = startupScript;
    }

    /** Set the name of the merge script. */
    public void setStageMergeScript(String stageMergeScript)
    {
        this.stageMergeScript = stageMergeScript;
    }

    /** Set the schema prefix for staging tables. */
    public void setStageSchemaPrefix(String stageSchemaPrefix)
    {
        this.stageSchemaPrefix = stageSchemaPrefix;
    }

    public void setStageTablePrefix(String stageTablePrefix)
    {
        this.stageTablePrefix = stageTablePrefix;
    }

    /** Set the default name of the staging table primary key. */
    public void setStagePkeyColumn(String stagePkeyColumn)
    {
        this.stagePkeyColumn = stagePkeyColumn;
    }

    /** Set the prefix for staging table columns. */
    public void setStageColumnPrefix(String stageColumnPrefix)
    {
        this.stageColumnPrefix = stageColumnPrefix;
    }

    /** Set the name of the staging directory. */
    public void setStageDirectory(String stageDirectory)
    {
        this.stageDirectory = stageDirectory;
    }

    /** If true, clean up files automatically. */
    public void setCleanUpFiles(boolean cleanUpFiles)
    {
        this.cleanUpFiles = cleanUpFiles;
    }

    /** Sets the platform charset name. */
    public void setCharset(String charset)
    {
        this.charset = charset;
    }

    /** Sets the timezone. */
    public void setTimezone(String timezone)
    {
        this.timezone = timezone;
    }

    /** Sets the proper handling of a load mismatch. */
    public void setOnLoadMismatch(String onLoadMismatchString)
    {
        this.onLoadMismatch = LoadMismatch.valueOf(onLoadMismatchString);
    }

    /** If true, show commands in the log when loading batches. */
    public void setShowCommands(boolean showCommands)
    {
        this.showCommands = showCommands;
    }

    /**
     * Applies row updates using a batch loading scheme. Statements are
     * discarded. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.RawApplier#apply(com.continuent.tungsten.replicator.event.DBMSEvent,
     *      com.continuent.tungsten.replicator.event.ReplDBMSHeader, boolean,
     *      boolean)
     */
    @Override
    public void apply(DBMSEvent event, ReplDBMSHeader header, boolean doCommit,
            boolean doRollback) throws ReplicatorException,
            ConsistencyException, InterruptedException
    {
        long seqno = header.getSeqno();
        ArrayList<DBMSData> dbmsDataValues = event.getData();

        // Apply heartbeat directly, skipping batch loading.
        String hbName = event
                .getMetadataOptionValue(ReplOptionParams.HEARTBEAT);
        if (hbName != null)
        {
            try
            {
                heartbeatTable.applyHeartbeat(conn, event.getSourceTstamp(),
                        hbName);
                heartbeatTable.completeHeartbeat(conn, header.getSeqno(),
                        event.getEventId());
                if (doCommit)
                {
                    conn.commit();
                }
            }
            catch (SQLException e)
            {
                throw new ReplicatorException("Error updating heartbeat table",
                        e);
            }
            return;
        }

        // Process consistency checks. These are currently not supported.
        String consistencyWhere = event
                .getMetadataOptionValue(ReplOptionParams.CONSISTENCY_WHERE);
        if (consistencyWhere != null)
        {
            logger.warn("Consistency checks are not supported: where clause="
                    + consistencyWhere);
            return;
        }

        // Iterate through values inferring the database name.
        for (DBMSData dbmsData : dbmsDataValues)
        {
            if (dbmsData instanceof StatementData)
            {
                if (logger.isDebugEnabled())
                {
                    StatementData stmtData = (StatementData) dbmsData;
                    logger.debug("Ignoring statement: " + stmtData.getQuery());
                }
            }
            else if (dbmsData instanceof RowChangeData)
            {
                RowChangeData rd = (RowChangeData) dbmsData;
                for (OneRowChange orc : rd.getRowChanges())
                {
                    // Get the action as well as the schema & table name.
                    ActionType action = orc.getAction();
                    String schema = orc.getSchemaName();
                    String table = orc.getTableName();
                    if (logger.isDebugEnabled())
                    {
                        logger.debug("Processing row update: action=" + action
                                + " schema=" + schema + " table=" + table);
                    }

                    // Process the action.
                    if (action.equals(ActionType.INSERT))
                    {
                        // Fetch column names and values.
                        List<ColumnSpec> colSpecs = orc.getColumnSpec();
                        ArrayList<ArrayList<ColumnVal>> colValues = orc
                                .getColumnValues();
                        // PK should be put in here by the PrimaryKeyFilter.
                        List<ColumnSpec> keySpecs = orc.getKeySpec();

                        // Get information the table definition.
                        Table tableMetadata = this.getTableMetadata(schema,
                                table, colSpecs, keySpecs);

                        // Insert each column into the CSV file.
                        writeValues(seqno, tableMetadata, colSpecs, colValues,
                                INSERT);
                    }
                    else if (action.equals(ActionType.UPDATE))
                    {
                        // Fetch column names and values.
                        List<ColumnSpec> keySpecs = orc.getKeySpec();
                        List<ColumnSpec> colSpecs = orc.getColumnSpec();
                        ArrayList<ArrayList<ColumnVal>> keyValues = orc
                                .getKeyValues();
                        ArrayList<ArrayList<ColumnVal>> colValues = orc
                                .getColumnValues();

                        // Get information the table definition.
                        Table tableMetadata = this.getTableMetadata(schema,
                                table, colSpecs, keySpecs);

                        // Write keys for deletion and columns for insert.
                        writeValues(seqno, tableMetadata, keySpecs, keyValues,
                                DELETE);
                        writeValues(seqno, tableMetadata, colSpecs, colValues,
                                INSERT);
                    }
                    else if (action.equals(ActionType.DELETE))
                    {
                        // Fetch column names and values.
                        List<ColumnSpec> keySpecs = orc.getKeySpec();
                        List<ColumnSpec> colSpecs = orc.getColumnSpec();
                        ArrayList<ArrayList<ColumnVal>> keyValues = orc
                                .getKeyValues();

                        // Get information about the table definition.
                        Table tableMetadata = this.getTableMetadata(schema,
                                table, colSpecs, keySpecs);

                        // Insert each column into the CSV file.
                        writeValues(seqno, tableMetadata, keySpecs, keyValues,
                                DELETE);
                    }
                    else
                    {
                        logger.warn("Unrecognized action type: " + action);
                        return;
                    }
                }
            }
            else if (dbmsData instanceof LoadDataFileFragment)
            {
                if (logger.isDebugEnabled())
                    logger.debug("Ignoring load data file fragment");
            }
            else if (dbmsData instanceof RowIdData)
            {
                if (logger.isDebugEnabled())
                    logger.debug("Ignoring row ID data");
            }
            else
            {
                logger.warn("Unsupported DbmsData class: "
                        + dbmsData.getClass().getName());
            }
        }

        // Mark the current header and commit position if requested.
        this.latestHeader = header;
        if (doCommit)
            commit();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.RawApplier#commit()
     */
    @Override
    public void commit() throws ReplicatorException, InterruptedException
    {
        // If we don't have a last header, there is nothing to be done.
        if (latestHeader == null)
        {
            if (logger.isDebugEnabled())
                logger.debug("Unable to commit; last header is null");
            return;
        }

        // Flush open CSV files now so that data become visible in case we
        // abort.
        for (CsvInfo info : openCsvFiles.values())
        {
            flush(info);
        }

        // Load each open CSV file.
        int loadCount = 0;
        for (CsvInfo info : openCsvFiles.values())
        {
            clearStageTable(info);
            mergeFromStageTable(info);
            loadCount++;
        }

        // Make sure the loaded CSV files match the total open files.
        if (loadCount != openCsvFiles.size())
        {
            throw new ReplicatorException(
                    "Load file counts do not match total: insert+delete="
                            + loadCount + " total=" + openCsvFiles.size());
        }

        // Update trep_commit_seqno.
        try
        {
            commitSeqnoTable
                    .updateLastCommitSeqno(taskId, this.latestHeader, 0);
        }
        catch (SQLException e)
        {
            throw new ReplicatorException("Unable to update commit position", e);
        }

        // SQL commit here.
        try
        {
            conn.commit();
            conn.setAutoCommit(false);
        }
        catch (SQLException e)
        {
            throw new ReplicatorException("Unable to commit transaction", e);
        }

        // Clear the CSV file cache.
        openCsvFiles.clear();

        // Clear the load directories if desired.
        if (cleanUpFiles)
            purgeDirIfExists(stageDir, false);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.RawApplier#getLastEvent()
     */
    @Override
    public ReplDBMSHeader getLastEvent() throws ReplicatorException,
            InterruptedException
    {
        return latestHeader;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.RawApplier#rollback()
     */
    @Override
    public void rollback() throws InterruptedException
    {
        // Roll back connection.
        try
        {
            rollbackTransaction();
        }
        catch (SQLException e)
        {
            logger.info("Unable to roll back transaction");
            if (logger.isDebugEnabled())
                logger.debug("Transaction rollback error", e);
        }

        // Clear the CSV file cache.
        openCsvFiles.clear();

        // Clear the load directories.
        try
        {
            purgeDirIfExists(stageDir, false);
        }
        catch (ReplicatorException e)
        {
            logger.error(
                    "Unable to purge staging directory; "
                            + stageDir.getAbsolutePath(), e);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.RawApplier#setTaskId(int)
     */
    @Override
    public void setTaskId(int id)
    {
        this.taskId = id;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    @Override
    public void configure(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        // Ensure basic properties are not null.
        assertNotNull(url, "url");
        assertNotNull(user, "user");
        assertNotNull(password, "password");
        assertNotNull(stageDirectory, "stageDirectory");
        assertNotNull(stageTablePrefix, "stageTablePrefix");
        assertNotNull(stageColumnPrefix, "stageRowIdColumn");
        assertNotNull(stageMergeScript, "stageMergeScript");

        // Get metadata schema.
        metadataSchema = context.getReplicatorSchemaName();
        consistencyTable = metadataSchema + "." + ConsistencyTable.TABLE_NAME;
        consistencySelect = "SELECT * FROM " + consistencyTable + " ";
    }

    // Ensure value is not null.
    public void assertNotNull(String property, String name)
            throws ReplicatorException
    {
        if (property == null)
        {
            throw new ReplicatorException(String.format(
                    "Property %s may not be null", name));
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    @Override
    public void prepare(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        // Create a formatter for printing dates.
        TimeZone tz = TimeZone.getTimeZone(timezone);
        dateFormatter = new SimpleDateFormat();
        dateFormatter.setTimeZone(tz);
        dateFormatter.applyPattern("yyyy-MM-dd HH:mm:ss.SSS");

        // Look up the output character set.
        if (charset == null)
            outputCharset = Charset.defaultCharset();
        else
        {
            try
            {
                outputCharset = Charset.forName(charset);
            }
            catch (Exception e)
            {
                throw new ReplicatorException("Unable to load character set: "
                        + charset, e);
            }
        }
        if (logger.isDebugEnabled())
        {
            logger.debug("Using output character set:"
                    + outputCharset.toString());
        }

        // Initialize script for merge operations.
        mergeScript = new BatchScript();
        mergeScript.load(new File(stageMergeScript));

        // Set up the staging directory.
        File staging = new File(stageDirectory);
        createDirIfNotExist(staging);

        // Define and create the load sub-directory.
        stageDir = new File(staging, "staging" + taskId);
        purgeDirIfExists(stageDir, true);
        createDirIfNotExist(stageDir);

        // Initialize table metadata cache.
        fullMetadataCache = new TableMetadataCache(5000);

        // Connect to DBMS.
        try
        {
            // Load driver if provided.
            if (driver != null)
            {
                try
                {
                    Class.forName(driver);
                }
                catch (Exception e)
                {
                    throw new ReplicatorException("Unable to load driver: "
                            + driver, e);
                }
            }

            // Create the database.
            if (!context.isPrivilegedSlaveUpdate())
            {
                logger.info("Assuming non-privileged JDBC login for apply");
            }
            conn = DatabaseFactory.createDatabase(url, user, password,
                    context.isPrivilegedSlaveUpdate());
            conn.connect(false);
            statement = conn.createStatement();

            // Get the table type. For MySQL databases this is important.
            String tableType = context.getTungstenTableType();

            // Set up heartbeat table.
            heartbeatTable = new HeartbeatTable(
                    context.getReplicatorSchemaName(), tableType);
            heartbeatTable.initializeHeartbeatTable(conn);

            // Create consistency table
            Table consistency = ConsistencyTable
                    .getConsistencyTableDefinition(metadataSchema);
            conn.createTable(consistency, false, tableType);

            // Set up commit seqno table and fetch the last processed event.
            commitSeqnoTable = new CommitSeqnoTable(conn,
                    context.getReplicatorSchemaName(), tableType, false);
            commitSeqnoTable.prepare(taskId);
            latestHeader = commitSeqnoTable.lastCommitSeqno(taskId);

            // Ensure we are not in auto-commit mode.
            conn.setAutoCommit(false);

        }
        catch (SQLException e)
        {
            String message = String.format("Failed using url=%s, user=%s", url,
                    user);
            throw new ReplicatorException(message, e);
        }

        // If a start-up script is present, execute that now.
        if (startupScript != null)
        {
            // Parse script.
            SqlScriptGenerator generator = initializeGenerator(this.startupScript);
            List<String> startCommands = generator
                    .getParameterizedScript(new HashMap<String, String>());

            // Execute commands.
            for (String startCommand : startCommands)
            {
                if (logger.isDebugEnabled())
                {
                    logger.debug("Executing start command: " + startCommand);
                }
                try
                {
                    long start = System.currentTimeMillis();
                    statement.execute(startCommand);
                    double interval = (System.currentTimeMillis() - start) / 1000.0;
                    if (logger.isDebugEnabled())
                    {
                        logger.debug("Execution completed: duration="
                                + interval + "s");
                    }
                }
                catch (SQLException e)
                {
                    ReplicatorException re = new ReplicatorException(
                            "Unable to execute load command", e);
                    re.setExtraData(startCommand);
                    throw re;
                }
            }
        }
    }

    // Initializes a SqlScriptGenerator.
    public static SqlScriptGenerator initializeGenerator(String script)
            throws ReplicatorException
    {
        FileReader fileReader = null;
        SqlScriptGenerator generator = new SqlScriptGenerator();
        try
        {
            File loadScriptFile = new File(script);
            fileReader = new FileReader(loadScriptFile);
            generator.load(fileReader);
        }
        catch (FileNotFoundException e)
        {
            throw new ReplicatorException("Unable to open load script file: "
                    + script);
        }
        catch (IOException e)
        {
            throw new ReplicatorException("Unable to read load script file: "
                    + script, e);
        }
        finally
        {
            if (fileReader != null)
            {
                try
                {
                    fileReader.close();
                }
                catch (IOException e)
                {
                }
            }
        }

        return generator;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    @Override
    public void release(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        // Release staging directory if cleanup is requested.
        if (stageDir != null && cleanUpFiles)
        {
            purgeDirIfExists(stageDir, true);
            stageDir = null;
        }

        // Release table cache.
        if (fullMetadataCache != null)
        {
            fullMetadataCache.invalidateAll();
            fullMetadataCache = null;
        }

        // Release our connection. This prevents all manner of trouble.
        if (conn != null)
        {
            conn.close();
            conn = null;
        }
    }

    /**
     * Create stage table definition by prefixing the base name, removing PK
     * constraint (if requested) and adding a stageRowIdColumn column.
     * 
     * @param baseTable Table for which to create corresponding stage table
     * @return Stage table definition
     */
    private Table getStageTable(Table baseTable)
    {
        // Generate schema and table names.
        String stageSchema = baseTable.getSchema();
        if (stageSchemaPrefix != null)
            stageSchema = stageSchemaPrefix + baseTable.getSchema();
        String stageName = stageTablePrefix + baseTable.getName();

        // Create table definition.
        Table stageTable = new Table(stageSchema, stageName);
        stageTable.setTable(stageName);
        stageTable.setSchema(stageSchema);

        // Opcode, seqno, and row_id columns are always first 3 columns.
        Column opCol = new Column(stageColumnPrefix + "opcode", Types.CHAR, 1);
        stageTable.AddColumn(opCol);
        Column seqnoCol = new Column(stageColumnPrefix + "seqno", Types.INTEGER);
        stageTable.AddColumn(seqnoCol);
        Column rowIdCol = new Column(stageColumnPrefix + "row_id",
                Types.INTEGER);
        stageTable.AddColumn(rowIdCol);

        // Add columns from base table.
        for (Column col : baseTable.getAllColumns())
        {
            stageTable.AddColumn(col);
        }

        // Return the result.
        return stageTable;
    }

    // Returns an open CSV file corresponding to a given schema, table name, and
    // load type.
    private CsvInfo getCsvWriter(Table tableMetadata)
            throws ReplicatorException
    {
        Table stageTableMetadata = getStageTable(tableMetadata);

        // Create a key.
        String key = tableMetadata.getSchema() + "." + tableMetadata.getName();
        CsvInfo info = this.openCsvFiles.get(key);
        if (info == null)
        {
            // Generate file name.
            File file = new File(this.stageDir, key + ".csv");

            // Pick the right table to use. For staging tables, we
            // need to use the stage metadata instead of going direct.
            Table csvMetadata;
            if (stageTableMetadata == null)
                csvMetadata = tableMetadata;
            else
                csvMetadata = stageTableMetadata;

            // Now generate the CSV writer.
            try
            {
                // Ensure the file does not exist. This cleans up from
                // previous transactions.
                if (file.exists())
                {
                    file.delete();
                }
                if (file.exists())
                {
                    throw new ReplicatorException(
                            "Unable to delete CSV file prior to loading new data: "
                                    + file.getAbsolutePath());
                }

                // Generate a CSV writer on the file.
                FileOutputStream outputStream = new FileOutputStream(file);
                OutputStreamWriter streamWriter = new OutputStreamWriter(
                        outputStream, outputCharset);
                BufferedWriter output = new BufferedWriter(streamWriter);
                CsvWriter writer = conn.getCsvWriter(output);
                writer.setNullAutofill(true);

                // Populate columns. The last column is the row ID, which is
                // automatically populated by the CSV writer.
                String rowIdName = stageColumnPrefix + "row_id";
                List<Column> columns = csvMetadata.getAllColumns();
                for (int i = 0; i < columns.size(); i++)
                {
                    Column col = columns.get(i);
                    String name = col.getName();
                    if (rowIdName.equals(name))
                        writer.addRowIdName(name);
                    else
                        writer.addColumnName(name);
                }

                // Create and cache writer information.
                info = new CsvInfo(this.stagePkeyColumn);
                info.schema = tableMetadata.getSchema();
                info.table = tableMetadata.getName();
                info.baseTableMetadata = tableMetadata;
                info.stageTableMetadata = stageTableMetadata;
                info.file = file;
                info.writer = writer;
                openCsvFiles.put(key, info);
            }
            catch (CsvException e)
            {
                throw new ReplicatorException("Unable to intialize CSV file: "
                        + e.getMessage(), e);
            }
            catch (IOException e)
            {
                throw new ReplicatorException("Unable to intialize CSV file: "
                        + file.getAbsolutePath());
            }
        }
        return info;
    }

    // Write values into a CSV file.
    private void writeValues(long seqno, Table tableMetadata,
            List<ColumnSpec> colSpecs,
            ArrayList<ArrayList<ColumnVal>> colValues, String opcode)
            throws ReplicatorException
    {
        CsvInfo info = getCsvWriter(tableMetadata);
        CsvWriter csv = info.writer;

        try
        {
            // Iterate over updates.
            Iterator<ArrayList<ColumnVal>> colIterator = colValues.iterator();
            while (colIterator.hasNext())
            {
                // Insert the sequence number and opcode.
                int csvIndex = 1;
                csv.put(csvIndex++, opcode);
                csv.put(csvIndex++, new Long(seqno).toString());

                // Now add the row data. Note that we skip the 3rd column as
                // that has the row_id value and is filled in automatically.
                ArrayList<ColumnVal> row = colIterator.next();
                for (int i = 0; i < row.size(); i++)
                {
                    ColumnVal columnVal = row.get(i);
                    ColumnSpec columnSpec = colSpecs.get(i);
                    String value = getCsvString(columnVal, columnSpec);
                    int colIdx = columnSpec.getIndex();
                    csv.put(colIdx + 3, value);
                }
                csv.write();
            }
        }
        catch (CsvException e)
        {
            // Enumerate table columns.
            StringBuffer colBuffer = new StringBuffer();
            for (Column col : info.baseTableMetadata.getAllColumns())
            {
                if (colBuffer.length() > 0)
                    colBuffer.append(",");
                colBuffer.append(col.getName());
            }

            // Enumerate CSV columns.
            StringBuffer csvBuffer = new StringBuffer();
            for (String name : csv.getNames())
            {
                if (csvBuffer.length() > 0)
                    csvBuffer.append(",");
                csvBuffer.append(name);
            }

            throw new ReplicatorException("Invalid write to CSV file: name="
                    + info.file.getAbsolutePath() + " table="
                    + info.baseTableMetadata + " table_columns="
                    + colBuffer.toString() + " csv_columns="
                    + csvBuffer.toString(), e);
        }
        catch (IOException e)
        {
            throw new ReplicatorException(
                    "Unable to append value to CSV file: "
                            + info.file.getAbsolutePath(), e);
        }
    }

    // Flush an open CSV file.
    private void flush(CsvInfo info) throws ReplicatorException
    {
        // Flush and close the file.
        try
        {
            info.writer.flush();
            info.writer.getWriter().close();
        }
        catch (CsvException e)
        {
            throw new ReplicatorException("Unable to close CSV file: "
                    + info.file.getAbsolutePath(), e);
        }
        catch (IOException e)
        {
            throw new ReplicatorException("Unable to close CSV file: "
                    + info.file.getAbsolutePath());
        }
    }

    // Load an open CSV file.
    protected void clearStageTable(CsvInfo info) throws ReplicatorException
    {
        Table table = info.stageTableMetadata;
        if (logger.isDebugEnabled())
        {
            logger.debug("Clearing stage table: " + table.fullyQualifiedName());
        }

        // Generate and submit SQL command.
        String delete = "DELETE FROM " + table.fullyQualifiedName();
        if (logger.isDebugEnabled())
        {
            logger.debug("Executing delete command: " + delete);
        }
        try
        {
            int rowsLoaded = statement.executeUpdate(delete);
            if (logger.isDebugEnabled())
            {
                logger.debug("Rows deleted: " + rowsLoaded);
            }
        }
        catch (SQLException e)
        {
            ReplicatorException re = new ReplicatorException(
                    "Unable to delete data from stage table: "
                            + table.fullyQualifiedName(), e);
            re.setExtraData(delete);
            throw re;
        }
    }

    // Load an open CSV file.
    private void mergeFromStageTable(CsvInfo info) throws ReplicatorException
    {
        Table base = info.baseTableMetadata;
        Table stage = info.stageTableMetadata;
        if (logger.isDebugEnabled())
        {
            logger.debug("Merging from stage table: "
                    + stage.fullyQualifiedName());
        }

        // Get the commands(s) to merge from stage table to base table.
        Map<String, String> parameters = info.getSqlParameters();
        List<BatchCommand> commands = mergeScript
                .getParameterizedScript(parameters);

        // Execute merge commands one by one.
        for (BatchCommand command : commands)
        {
            if (logger.isDebugEnabled())
            {
                logger.debug("Executing merge command: " + command.toString());
            }
            String commandText = command.getCommand();
            if (this.showCommands)
            {
                logger.info("Batch Command: " + commandText);
            }

            // Process command.
            long start = System.currentTimeMillis();
            if (commandText.startsWith("!"))
            {
                // Check for "bang" with no command following...
                if (commandText.length() <= 1)
                {
                    // This must be ignored.
                    continue;
                }

                String osCommandText = commandText.substring(1);
                String[] osArray = {"sh", "-c", osCommandText};
                ProcessExecutor pe = new ProcessExecutor();
                pe.setCommands(osArray);
                if (logger.isDebugEnabled())
                {
                    logger.debug("Executing OS command: " + osCommandText);
                }
                pe.run();
                if (logger.isDebugEnabled())
                {
                    logger.debug("OS command stdout: " + pe.getStdout());
                    logger.debug("OS command stderr: " + pe.getStderr());
                    logger.debug("OS command exit value: " + pe.getExitValue());
                }
                if (!pe.isSuccessful())
                {
                    logger.error("OS command failed: command=" + osCommandText
                            + " rc=" + pe.getExitValue() + " stdout="
                            + pe.getStdout() + " stderr=" + pe.getStderr());
                    throw new ReplicatorException("OS command failed: command="
                            + osCommandText);
                }
            }
            else
            {
                // SQL command.
                try
                {
                    int rows = statement.executeUpdate(commandText);
                    if (logger.isDebugEnabled())
                    {
                        logger.debug("SQL execution completed: rows updated="
                                + rows);
                    }
                }
                catch (SQLException e)
                {
                    ReplicatorException re = new ReplicatorException(
                            "Unable to merge data to base table: "
                                    + base.fullyQualifiedName(), e);
                    re.setExtraData(command.toString());
                    throw re;
                }
            }

            double interval = (System.currentTimeMillis() - start) / 1000.0;
            if (logger.isDebugEnabled())
            {
                logger.debug("Execution completed: duration=" + interval + "s");
            }
        }
    }

    // Get full table metadata. Cache for table metadata is populated
    // automatically.
    private Table getTableMetadata(String schema, String name,
            List<ColumnSpec> colSpecs, List<ColumnSpec> keySpecs)
    {
        // In the cache first.
        Table t = fullMetadataCache.retrieve(schema, name);

        // Create if missing and add to cache.
        if (t == null)
        {
            // Create table definition.
            t = new Table(schema, name);

            // Add column definitions.
            for (ColumnSpec colSpec : colSpecs)
            {
                Column col = new Column(colSpec.getName(), colSpec.getType());
                t.AddColumn(col);
            }

            // Store the new definition.
            fullMetadataCache.store(t);
            if (logger.isDebugEnabled())
            {
                logger.debug("Added metadata for table: schema=" + schema
                        + " table=" + name + " metadata="
                        + t.toExtendedString());
            }
        }

        // If keys are missing and we have them, add them now. This extra
        // step is necessary because insert operations do not have keys,
        // whereas update and delete do. So if we added the insert later,
        // we will need it now.
        if (t.getKeys().size() == 0 && keySpecs != null && keySpecs.size() > 0)
        {
            // Fetch the column definition matching each element of the key
            // we receive from replication and construct a key definition.
            Key key = new Key(Key.Primary);
            for (ColumnSpec keySpec : keySpecs)
            {
                for (Column col : t.getAllColumns())
                {
                    String colName = col.getName();
                    if (colName != null && colName.equals(keySpec.getName()))
                    {
                        key.AddColumn(col);
                        break;
                    }
                }
            }

            // Add the key.
            t.AddKey(key);
            if (logger.isDebugEnabled())
            {
                logger.debug("Added keys for table: schema=" + schema
                        + " table=" + name + " metadata="
                        + t.toExtendedString());
            }
        }

        // Return the table.
        return t;
    }

    /**
     * Converts a column value to a suitable String for CSV loading. This can be
     * overloaded for particular DBMS types.
     * 
     * @param columnVal Column value
     * @param columnSpec Column metadata
     * @return String for loading
     * @throws CsvException
     */
    protected String getCsvString(ColumnVal columnVal, ColumnSpec columnSpec)
            throws CsvException
    {
        Object value = columnVal.getValue();
        if (value == null)
        {
            return null;
        }
        else if (value instanceof Timestamp)
        {
            return dateFormatter.format((Timestamp) value);
        }
        else if (value instanceof java.sql.Date)
        {
            return dateFormatter.format((java.sql.Date) value);
        }
        else if (columnSpec.getType() == Types.BLOB
                || (columnSpec.getType() == Types.NULL && columnVal.getValue() instanceof SerialBlob))
        { // ______^______
          // Blob in the incoming event masked as NULL,
          // though this happens with a non-NULL value!
          // Case targeted with this: MySQL.TEXT -> CSV

            SerialBlob blob = (SerialBlob) columnVal.getValue();

            if (columnSpec.isBlob())
            {
                // If it's really a blob, the following will not work correctly,
                // but let's not eat the value, if there's a possibility of one.
                return value.toString();
            }
            else
            {
                // Expect a textual field.
                String toString = null;

                if (blob != null)
                {
                    try
                    {
                        toString = new String(blob.getBytes(1,
                                (int) blob.length()));
                    }
                    catch (SerialException e)
                    {
                        throw new CsvException(
                                "Execption while getting blob.getBytes(...)", e);
                    }
                }

                return toString;
            }
        }
        else
            return value.toString();
    }

    // Create a directory if it does not exist.
    private void createDirIfNotExist(File dir) throws ReplicatorException
    {
        if (!dir.exists())
        {
            if (!dir.mkdirs())
            {
                throw new ReplicatorException(
                        "Unable to create staging directory: "
                                + dir.getAbsolutePath());
            }
        }
    }

    // Clear and optionally delete a directory if it exists.
    private void purgeDirIfExists(File dir, boolean delete)
            throws ReplicatorException
    {
        // Return if there's nothing to do.
        if (!dir.exists())
            return;

        // Remove any files.
        for (File child : dir.listFiles())
        {
            if (!child.delete())
            {
                throw new ReplicatorException("Unable to delete staging file: "
                        + child.getAbsolutePath());
            }
        }

        // Remove directory if desired.
        if (delete && !dir.delete())
        {
            if (!dir.delete())
            {
                throw new ReplicatorException(
                        "Unable to delete staging directory: "
                                + dir.getAbsolutePath());
            }
        }
    }

    // Rolls back the current transaction.
    private void rollbackTransaction() throws SQLException
    {
        try
        {
            conn.rollback();
        }
        catch (SQLException e)
        {
            logger.error("Failed to rollback : " + e);
            throw e;
        }
        finally
        {
            // Switch connection back to auto-commit.
            conn.setAutoCommit(true);
        }
    }
}