/**
 * Tungsten Scale-Out Stack
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
 * Contributor(s): Stephane Giron
 */

package com.continuent.tungsten.replicator.thl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import javax.sql.rowset.serial.SerialBlob;

import org.apache.log4j.Logger;

import com.continuent.tungsten.commons.config.TungstenProperties;
import com.continuent.tungsten.commons.exec.ArgvIterator;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntimeConf;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.LoadDataFileFragment;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.dbms.RowIdData;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.event.DBMSEmptyEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSFilteredEvent;
import com.continuent.tungsten.replicator.event.ReplEvent;
import com.continuent.tungsten.replicator.event.ReplOption;
import com.continuent.tungsten.replicator.thl.log.DiskLog;
import com.continuent.tungsten.replicator.thl.log.LogConnection;

/**
 * This class defines a THLManagerCtrl that implements a utility to access
 * THLManager methods. See the printHelp() command for a description of current
 * commands.
 * 
 * @author <a href="mailto:linas.virbalas@continuent.com">Linas Virbalas</a>
 * @version 1.0
 */
public class THLManagerCtrl
{
    private static Logger         logger             = Logger.getLogger(THLManagerCtrl.class);
    /**
     * Default path to replicator.properties if user not specified other.
     */
    protected static final String defaultConfigPath  = ".."
                                                             + File.separator
                                                             + "conf"
                                                             + File.separator
                                                             + "static-default.properties";

    /**
     * Maximum length of characters to print out for a BLOB. If BLOB is larger,
     * it is truncated and "<...>" is added to the end.<br/>
     * TODO: make configurable from somewhere.
     */
    private static final int      maxBlobPrintLength = 1000;

    protected static ArgvIterator argvIterator       = null;

    protected String              configFile         = null;

    private String                logDir;

    private DiskLog               diskLog;

    /**
     * Creates a new <code>THLManagerCtrl</code> object.
     * 
     * @param configFile Path to the Tungsten properties file.
     * @throws Exception
     */
    public THLManagerCtrl(String configFile) throws Exception
    {
        // Set path to configuration file.
        this.configFile = configFile;

        // Read properties required to connect to database.
        TungstenProperties properties = readConfig();
        logDir = properties.getString("replicator.store.thl.log_dir");
    }

    /**
     * Reads the replicator.properties.
     */
    protected TungstenProperties readConfig() throws Exception
    {
        TungstenProperties conf = null;

        // Open configuration file.
        File propsFile = new File(configFile);
        if (!propsFile.exists() || !propsFile.canRead())
        {
            throw new Exception("Properties file not found: "
                    + propsFile.getAbsolutePath(), null);
        }
        conf = new TungstenProperties();

        // Read configuration.
        try
        {
            conf.load(new FileInputStream(propsFile));
        }
        catch (IOException e)
        {
            throw new Exception(
                    "Unable to read properties file: "
                            + propsFile.getAbsolutePath() + " ("
                            + e.getMessage() + ")", null);
        }
        return conf;
    }

    /**
     * Connect to the underlying database containing THL.
     * 
     * @throws THLException
     */
    public void connect(boolean readOnly) throws ReplicatorException, InterruptedException
    {
        diskLog = new DiskLog();
        diskLog.setLogDir(logDir);
        diskLog.setReadOnly(readOnly);
        diskLog.prepare();
    }

    /**
     * Disconnect from the THL database.
     */
    public void disconnect()
    {
        if (diskLog != null)
        {
            try
            {
                diskLog.release();
            }
            catch (ReplicatorException e)
            {
                logger.warn("Unable to release log", e);
            }
            catch (InterruptedException e)
            {
                logger.warn("Unexpected interruption while closing log", e);
            }
            diskLog = null;
        }
    }

    /**
     * Queries THL for summary information.
     * 
     * @return Info holder
     * @throws THLException
     */
    public InfoHolder getInfo() throws THLException
    {
        long minSeqno = diskLog.getMinSeqno();
        long maxSeqno = diskLog.getMaxSeqno();
        return new InfoHolder(minSeqno, maxSeqno, maxSeqno - minSeqno, -1);
    }
    
    
    /**
     * Formats column and column value for printing.
     * 
     * @param charset character set name to be used to decode byte arrays in row
     *            replication
     */
    public static String formatColumn(OneRowChange.ColumnSpec colSpec,
            OneRowChange.ColumnVal value, String prefix, String charset)
    {
        String log = "  - " + prefix + "(";
        if (colSpec != null)
            log += colSpec.getIndex() + ": " + colSpec.getName();
        log += ") = ";
        if (value != null)
            if (value.getValue() != null)
            {
                if (value.getValue() instanceof SerialBlob)
                {
                    try
                    {
                        SerialBlob blob = (SerialBlob) value.getValue();
                        String blobString = new String(blob.getBytes(1,
                                maxBlobPrintLength));
                        log += blobString;
                        if (blob.length() > maxBlobPrintLength)
                            log += "<...>";
                    }
                    catch (Exception e)
                    {
                        log += value.getValue().toString();
                    }
                }
                else if (value.getValue() instanceof byte[] && charset != null)
                {
                    try
                    {
                        log += new String((byte[]) value.getValue(), charset);
                    }
                    catch (UnsupportedEncodingException e)
                    {
                        logger.warn("Unsupported encoding " + charset, e);
                    }
                }
                else
                    log += value.getValue().toString();
            }
            else
                log += "NULL";
        else
            log += "NULL";
        return log;
    }

    /**
     * Format and print schema name if it differs from the last printed schema
     * name.
     * 
     * @param sb StringBuilder on which to print
     * @param schema Schema name to print.
     * @param lastSchema Last printed schema name.
     * @param pureSQL If true, use pure SQL output. Formatted form otherwise.
     * @return Schema name as given in the schema parameter.
     */
    private static String printSchema(StringBuilder sb, String schema,
            String lastSchema, boolean pureSQL)
    {
        if (schema != null
                && (lastSchema == null || (lastSchema != null && lastSchema
                        .compareTo(schema) != 0)))
        {
            if (pureSQL) // TODO: what about Oracle and `USE`?
                println(sb, "USE " + schema + ";");
            else
                println(sb, "- SCHEMA = " + schema);
        }
        return schema;
    }

    /**
     * List THL events within the given range.
     * 
     * @param low Sequence number specifying the beginning of the range. Leave
     *            null to start from the very beginning of the table.
     * @param high Sequence number specifying the end of the range. Leave null
     *            to end at the very end of the table.
     * @param by
     * @param pureSQL Output events in the pure SQL form if true, formatted form
     *            otherwise.
     * @param charset character set name to be used to decode byte arrays in row
     *            replication
     * @throws THLException
     * @throws InterruptedException 
     */
    public void listEvents(Long low, Long high, Long by, boolean pureSQL,
            String charset) throws THLException, InterruptedException
    {
        long lowIndex = 0;
        long minSeqno = diskLog.getMinSeqno();
        if (low != null && low >= minSeqno)
            lowIndex = low;
        else
            lowIndex = minSeqno;

        Long highIndex;
        long maxSeqno = diskLog.getMaxSeqno();
        if (high != null && high <= maxSeqno)
            highIndex = high;
        else
            highIndex = maxSeqno;

        // Find low value. 
        LogConnection conn = diskLog.connect(true);
        if (! conn.seek(lowIndex))
        {
            logger.error("Unable to find sequence number: " + lowIndex);
            fail();
        }

        // Iterate until we run out of sequence numbers.  
        THLEvent thlEvent = null;
        while (lowIndex <= highIndex && (thlEvent = conn.next(false)) != null)
        {
            lowIndex = thlEvent.getSeqno();
            if (!pureSQL)
            {
                StringBuilder sb = new StringBuilder();
                printHeader(sb, thlEvent);
                print(sb.toString());
            }
            ReplEvent replEvent = thlEvent.getReplEvent();
            if (replEvent instanceof ReplDBMSEvent)
            {
                ReplDBMSEvent event = (ReplDBMSEvent) replEvent;
                StringBuilder sb = new StringBuilder();
                printReplDBMSEvent(sb, event, pureSQL, charset);
                print(sb.toString());
            }
            else
            {
                println("# " + replEvent.getClass().getName()
                        + ": not supported.");
            }
        }
        
        // Disconnect. 
        conn.release();
    }

    /**
     * Prints a formatted header into StringBuilder for the given THLEvent.
     * 
     * @param stringBuilder StringBuilder object to append formatted contents
     *            to.
     * @param thlEvent THLEvent to print out.
     * @see #printHeader(StringBuilder, ReplDBMSEvent)
     */
    public static void printHeader(StringBuilder stringBuilder,
            THLEvent thlEvent)
    {
        println(stringBuilder, "SEQ# = " + thlEvent.getSeqno() + " / FRAG# = "
                + thlEvent.getFragno()
                + (thlEvent.getLastFrag() ? (" (last frag)") : ""));
        println(stringBuilder, "- TIME = " + thlEvent.getSourceTstamp());
        println(stringBuilder, "- EPOCH# = " + thlEvent.getEpochNumber());
        println(stringBuilder, "- EVENTID = " + thlEvent.getEventId());
        println(stringBuilder, "- SOURCEID = " + thlEvent.getSourceId());
        if (thlEvent.getComment() != null && thlEvent.getComment().length() > 0)
            println(stringBuilder, "- COMMENTS = " + thlEvent.getComment());
    }

    /**
     * Prints a formatted header into StringBuilder for the given ReplDBMSEvent.
     * Note that ReplDBMSEvent doesn't contain eventId, thus it is not printed.
     * If you need to print eventId, use
     * {@link #printHeader(StringBuilder, THLEvent)}
     * 
     * @param stringBuilder StringBuilder object to append formatted contents
     *            to.
     * @param event ReplDBMSEvent to print out.
     * @see #printHeader(StringBuilder, THLEvent)
     */
    public static void printHeader(StringBuilder stringBuilder,
            ReplDBMSEvent event)
    {
        println(stringBuilder, "SEQ# = " + event.getSeqno());
        println(stringBuilder, "- TIME = "
                + event.getDBMSEvent().getSourceTstamp());
        println(stringBuilder, "- SOURCEID = " + event.getSourceId());
    }

    /**
     * Formats and prints ReplDBMSEvent into a given stringBuilder.
     * 
     * @param stringBuilder StringBuilder object to append formatted contents
     *            to.
     * @param event ReplDBMSEvent to print out.
     * @param pureSQL If true, use pure SQL output. Formatted form otherwise.
     * @param charset character set name to be used to decode byte arrays in row
     *            replication
     */
    public static void printReplDBMSEvent(StringBuilder stringBuilder,
            ReplDBMSEvent event, boolean pureSQL, String charset)
    {
        if (event == null)
        {
            println(stringBuilder, "- TYPE = null");
            return;
        }

        // Add metadata before handling specific types of ReplDBMSEvents.
        List<ReplOption> metadata = event.getDBMSEvent().getMetadata();
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (ReplOption option : metadata)
        {
            if (sb.length() > 1)
                sb.append(";");
            String value = option.getOptionValue();
            sb.append(option.getOptionName()).append(
                    (value != null && value.length() > 0 ? "=" + value : ""));
        }
        sb.append("]");
        println(stringBuilder, "- METADATA = " + sb.toString());
        println(stringBuilder, "- TYPE = " + event.getClass().getName());

        if (event.getDBMSEvent() instanceof DBMSEmptyEvent)
        {
            println(stringBuilder, "## Empty event ##");
            return;
        }

        if (event instanceof ReplDBMSFilteredEvent)
        {
            println(stringBuilder, "## Filtered events ##");
            println(stringBuilder, "From Seqno# " + event.getSeqno()
                    + " / Fragno# " + event.getFragno());
            println(stringBuilder,
                    "To Seqno# "
                            + ((ReplDBMSFilteredEvent) event).getSeqnoEnd()
                            + " / Fragno# "
                            + ((ReplDBMSFilteredEvent) event).getFragnoEnd());
            return;
        }

        ArrayList<DBMSData> data = event.getData();
        String lastSchema = null;
        for (int i = 0; i < data.size(); i++)
        {
            DBMSData dataElem = data.get(i);
            if (dataElem instanceof RowChangeData)
            {
                RowChangeData rowChange = (RowChangeData) dataElem;
                lastSchema = printRowChangeData(stringBuilder, rowChange,
                        lastSchema, pureSQL, i, charset);
            }
            else if (dataElem instanceof StatementData)
            {
                StatementData statement = (StatementData) dataElem;
                lastSchema = printStatementData(stringBuilder, statement,
                        lastSchema, pureSQL, i);
            }
            else if (dataElem instanceof RowIdData)
            {
                RowIdData rowid = (RowIdData) dataElem;
                lastSchema = printRowIdData(stringBuilder, rowid, lastSchema,
                        pureSQL, i);
            }
            else if (dataElem instanceof LoadDataFileFragment)
            {
                LoadDataFileFragment loadDataFileFragment = (LoadDataFileFragment) dataElem;
                String schema = loadDataFileFragment.getDefaultSchema();
                printSchema(stringBuilder, schema, lastSchema, pureSQL);
                lastSchema = schema;
                println("- DATA FILE #" + loadDataFileFragment.getFileID()
                        + " / size : " + loadDataFileFragment.getData().length);
            }
            else
                println(stringBuilder, "# " + dataElem.getClass().getName()
                        + ": not supported.");
        }
    }

    /**
     * Prints RowIdData event.
     * 
     * @param rowid RowidIdData object to format and print.
     * @param lastSchema Last printed schema name.
     * @param pureSQL If true, use pure SQL output. Formatted form otherwise.
     * @param sqlIndex Which SQL event is it.
     * @return Schema name.
     */
    private static String printRowIdData(StringBuilder stringBuilder,
            RowIdData rowid, String lastSchema, boolean pureSQL, int sqlIndex)
    {
        // Output actual DML/DDL statement.
        if (pureSQL)
        {
            println(stringBuilder, "SET INSERT_ID=" + rowid.getRowId());
        }
        else
        {
            println(stringBuilder, "- SQL(" + sqlIndex + ") = "
                    + "SET INSERT_ID=" + rowid.getRowId());
        }
        return lastSchema;
    }

    /**
     * Prints StatementData event.
     * 
     * @param statement StatementData object to format and print.
     * @param lastSchema Last printed schema name.
     * @param pureSQL If true, use pure SQL output. Formatted form otherwise.
     * @param sqlIndex Which SQL event is it.
     * @return Schema name.
     */
    private static String printStatementData(StringBuilder stringBuilder,
            StatementData statement, String lastSchema, boolean pureSQL,
            int sqlIndex)
    {
        // Output schema name if needed.
        String schema = statement.getDefaultSchema();
        printOptions(stringBuilder, statement);
        printSchema(stringBuilder, schema, lastSchema, pureSQL);
        String query = statement.getQuery();

        if (query == null)
            query = new String(statement.getQueryAsBytes());

        // Output actual DML/DDL statement.
        if (pureSQL)
        {
            println(stringBuilder, formatSQL(query));
        }
        else
        {
            println(stringBuilder, "- SQL(" + sqlIndex + ") = " + query);
        }
        return schema;
    }

    private static void printOptions(StringBuilder stringBuilder,
            StatementData statement)
    {
        if (statement.getOptions() != null)
            println(stringBuilder, "- OPTIONS = " + statement.getOptions());
    }

    /**
     * Prints RowChangeData event.
     * 
     * @param rowChange RowChangeData object to format and print.
     * @param lastSchema Last printed schema name.
     * @param pureSQL If true, use pure SQL output. Formatted form otherwise.
     * @param sqlIndex Which SQL event is it.
     * @param charset character set name to be used to decode byte arrays in row
     *            replication
     * @return Last printed schema name.
     */
    private static String printRowChangeData(StringBuilder stringBuilder,
            RowChangeData rowChange, String lastSchema, boolean pureSQL,
            int sqlIndex, String charset)
    {
        if (!pureSQL)
            println(stringBuilder, "- SQL(" + sqlIndex + ") =");
        String schema = null;
        for (OneRowChange oneRowChange : rowChange.getRowChanges())
        {
            // Output row change details.
            if (pureSQL)
                println(stringBuilder,
                        "# SQL output on row change events is not supported yet.");
            else
            {
                println(stringBuilder, " - ACTION = "
                        + oneRowChange.getAction().toString());
                println(stringBuilder,
                        " - SCHEMA = " + oneRowChange.getSchemaName());
                println(stringBuilder,
                        " - TABLE = " + oneRowChange.getTableName());
                ArrayList<OneRowChange.ColumnSpec> keys = oneRowChange
                        .getKeySpec();
                ArrayList<OneRowChange.ColumnSpec> columns = oneRowChange
                        .getColumnSpec();
                ArrayList<ArrayList<OneRowChange.ColumnVal>> keyValues = oneRowChange
                        .getKeyValues();
                ArrayList<ArrayList<OneRowChange.ColumnVal>> columnValues = oneRowChange
                        .getColumnValues();
                for (int row = 0; row < columnValues.size()
                        || row < keyValues.size(); row++)
                {
                    println(stringBuilder, " - ROW# = " + row);
                    // Print column values.
                    for (int c = 0; c < columns.size(); c++)
                    {
                        if (columnValues.size() > 0)
                        {
                            OneRowChange.ColumnSpec colSpec = columns.get(c);
                            ArrayList<OneRowChange.ColumnVal> values = columnValues
                                    .get(row);
                            OneRowChange.ColumnVal value = values.get(c);
                            println(stringBuilder,
                                    formatColumn(colSpec, value, "COL", charset));
                        }
                    }
                    // Print key values.
                    for (int k = 0; k < keys.size(); k++)
                    {
                        if (keyValues.size() > 0)
                        {
                            OneRowChange.ColumnSpec colSpec = keys.get(k);
                            ArrayList<OneRowChange.ColumnVal> values = keyValues
                                    .get(row);
                            OneRowChange.ColumnVal value = values.get(k);
                            println(stringBuilder,
                                    formatColumn(colSpec, value, "KEY", charset));
                        }
                    }
                }
            }
        }
        return schema;
    }

    /**
     * Formats the given SQL statement into an ANSI compatible form.
     * 
     * @param sql
     * @return Formatted SQL statement.
     */
    private static String formatSQL(String sql)
    {
        // TODO: expand this method.
        if (!sql.endsWith(";"))
            sql += ";";
        return sql;
    }

    /**
     * Purge THL events in the given seqno interval.
     * 
     * @param low Sequence number specifying the beginning of the range. Leave
     *            null to start from the very beginning of the table.
     * @param high Sequence number specifying the end of the range. Leave null
     *            to end at the very end of the table.
     * @throws THLException
     */
    public void purgeEvents(Long low, Long high) throws THLException
    {
        LogConnection conn = diskLog.connect(false);
        try
        {
            conn.delete(low, high);
            conn.release();
        }
        catch (InterruptedException e)
        {
            logger.warn("Delete operation was interrupted!");
        }
        logger.info("Transactions deleted");
    }

    /**
     * Main method to run utility.
     * 
     * @param argv optional command string
     */
    public static void main(String argv[])
    {
        try
        {
            // Command line parameters and options.
            String configFile = null;
            String service = null;
            String command = null;
            Long seqno = null;
            Long low = null;
            Long high = null;
            Long by = null;
            Boolean pureSQL = null;
            Boolean yesToQuestions = null;
            String fileName = null;
            String charsetName = null;

            // Parse command line arguments.
            argvIterator = new ArgvIterator(argv);
            String curArg = null;
            while (argvIterator.hasNext())
            {
                curArg = argvIterator.next();
                if ("-conf".equals(curArg))
                    configFile = argvIterator.next();
                else if ("-service".equals(curArg))
                    service = argvIterator.next();
                else if ("-seqno".equals(curArg))
                    seqno = Long.parseLong(argvIterator.next());
                else if ("-low".equals(curArg))
                    low = Long.parseLong(argvIterator.next());
                else if ("-high".equals(curArg))
                    high = Long.parseLong(argvIterator.next());
                else if ("-by".equals(curArg))
                    by = Long.parseLong(argvIterator.next());
                else if ("-sql".equals(curArg))
                    pureSQL = true;
                else if ("-y".equals(curArg))
                    yesToQuestions = true;
                else if ("-charset".equals(curArg))
                {
                    charsetName = argvIterator.next();
                    if (!Charset.isSupported(charsetName))
                    {
                        println("Unsupported charset " + charsetName
                                + ". Using default.");
                        charsetName = null;
                    }
                }
                else if ("-file".equals(curArg))
                {
                    fileName = argvIterator.next();
                }
                else if (curArg.startsWith("-"))
                    fatal("Unrecognized option: " + curArg, null);
                else
                    command = curArg;
            }

            // Construct actual THLManagerCtrl and call methods based on a
            // parsed user input.
            if (command == null)
            {
                println("Command is missing!");
                printHelp();
                fail();
            }
            else if (THLCommands.HELP.equals(command))
            {
                printHelp();
                succeed();
            }

            // Use default configuration file in case user didn't specify one.
            if (configFile == null)
            {
                if (service == null)
                {
                    fatal("You must specify either a config file or a service name (-conf or -service)",
                            null);
                }
                else
                {
                    ReplicatorRuntimeConf runtimeConf = ReplicatorRuntimeConf
                            .getConfiguration(service);
                    configFile = runtimeConf.getReplicatorProperties()
                            .getAbsolutePath();
                }
            }

            if (THLCommands.INFO.equals(command))
            {
                THLManagerCtrl thlManager = new THLManagerCtrl(configFile);
                thlManager.connect(true);

                InfoHolder info = thlManager.getInfo();
                println("min seq# = " +  info.getMinSeqNo());
                println("max seq# = " + info.getMaxSeqNo());
                println("events = " + info.getEventCount());
                println("highest known replicated seq# = "
                        + info.getHighestReplicatedEvent());

                thlManager.disconnect();
            }
            else if (THLCommands.LIST.equals(command))
            {
                THLManagerCtrl thlManager = new THLManagerCtrl(configFile);
                thlManager.connect(true);

                if (fileName != null)
                {
                    thlManager.listEvents(fileName, getBoolOrFalse(pureSQL),
                            charsetName);
                }
                else if (seqno == null)
                    thlManager.listEvents(low, high, by,
                            getBoolOrFalse(pureSQL), charsetName);
                else
                    thlManager.listEvents(seqno, seqno, by,
                            getBoolOrFalse(pureSQL), charsetName);

                thlManager.disconnect();
            }
            else if (THLCommands.PURGE.equals(command))
            {
                THLManagerCtrl thlManager = new THLManagerCtrl(configFile);
                thlManager.connect(false);

                println("WARNING: The purge command will break replication if you delete all events or delete events that have not reached all slaves.");

                boolean confirmed = true;
                if (!getBoolOrFalse(yesToQuestions))
                {
                    confirmed = false;
                    println("Are you sure you wish to delete these events [y/N]?");
                    if (readYes())
                        confirmed = true;
                    else
                        println("Nothing done.");
                }
                if (confirmed)
                {
                    String log = "Deleting events where";
                    int deleted = 0;
                    if (seqno == null)
                    {
                        if (low != null)
                            log += " SEQ# >= " + low;
                        if (low != null && high != null)
                            log += " and";
                        if (high != null)
                            log += " SEQ# <=" + high;
                        println(log);
                        thlManager.purgeEvents(low, high);
                    }
                    else
                    {
                        log += " SEQ# = " + seqno;
                        println(log);
                        thlManager.purgeEvents(seqno, seqno);
                    }
                    println("Deleted events: " + deleted);
                }

                thlManager.disconnect();
            }
            else if (THLCommands.SKIP.equals(command))
            {
                println("SKIP operation is no longer supported");
                println("Please check the ");
                return;
            }
            else if (command.equals("index"))
            {
                THLManagerCtrl thlManager = new THLManagerCtrl(configFile);
                thlManager.connect(true);

                thlManager.printIndex();

                thlManager.disconnect();
            }
            else
            {
                println("Unknown command: '" + command + "'");
                printHelp();
                fail();
            }
        }
        catch (Throwable t)
        {
            fatal("Fatal error: " + t.getMessage(), t);
        }
    }

    private void listEvents(String fileName, boolean pureSQL, String charset)
            throws ReplicatorException, IOException, InterruptedException
    {
        LogConnection conn = diskLog.connect(true);
        if (! conn.seek(fileName))
        {
            logger.error("File not found: " + fileName);
            fail();
        }
        THLEvent thlEvent = null;
        while ((thlEvent = conn.next(false)) != null)
        {
            if (!pureSQL)
            {
                StringBuilder sb = new StringBuilder();
                printHeader(sb, thlEvent);
                print(sb.toString());
            }
            ReplEvent replEvent = thlEvent.getReplEvent();
            if (replEvent instanceof ReplDBMSEvent)
            {
                ReplDBMSEvent event = (ReplDBMSEvent) replEvent;
                StringBuilder sb = new StringBuilder();
                printReplDBMSEvent(sb, event, pureSQL, charset);
                print(sb.toString());
            }
            else
            {
                println("# " + replEvent.getClass().getName()
                        + ": not supported.");
            }
        }
    }

    private void printIndex()
    {
        println(diskLog.getIndex());
    }

    protected static void printHelp()
    {
        println("Replicator THL Manager");
        println("Syntax: thl [global-options] command [command-options]");
        println("Global options:");
        println("  -conf path                       - Path to replicator.properties. Default:    ");
        println("                                     " + defaultConfigPath);
        println("age_format: Seconds=s, Minutes=m, hours=h, days=d e.g \"2d 4h\" is 2 days + 4 hours");
        println("Commands and corresponding options:");
        println("  list [-low #] [-high #] [-by #]  - Dump THL events from low to high #.        ");
        println("       [-sql]                        Specify -sql to use pure SQL output only.  ");
        println("       [-charset <charset_name>]     Character set used for decoding, when needed.");
        println("                                     (only with row replication with using_bytes_for_string is set to true).");
        println("  list [-seqno #] [-sql]           - Dump the exact event by a given #.         ");
        println("       [-charset <charset_name>]     Character set used for decoding, when needed.");
        println("  list [-file <file_name>] [-sql]  - Dump the content of the given file (for Disk-based storage).");
        println("       [-charset <charset_name>]     Character set used for decoding, when needed.");
        println("  index                            - Display index of Disk-based storage        ");
        println("  purge [-low #] [-high #] [-y]    - Delete events within the given range");
        println("  purge [-seqno #] [-y]            - Delete the exact event.                    ");
        println("                                     Use -y to answer yes to all questions.     ");
        println("  info                             - Display minimum, maximum sequence number   ");
        println("                                     and other summary.                         ");
        println("  help                             - Print this help information.               ");
    }

    /**
     * Appends a message to a given stringBuilder, adds a newline character at
     * the end.
     * 
     * @param msg String to print.
     * @param stringBuilder StringBuilder object to add a message to.
     */
    private static void println(StringBuilder stringBuilder, String msg)
    {
        stringBuilder.append(msg);
        stringBuilder.append("\n");
    }

    /**
     * Print a message to stdout with trailing new line character.
     * 
     * @param msg
     */
    protected static void println(String msg)
    {
        System.out.println(msg);
    }

    /**
     * Print a message to stdout without trailing new line character.
     * 
     * @param msg
     */
    protected static void print(String msg)
    {
        System.out.print(msg);
    }

    /**
     * Abort following a fatal error.
     * 
     * @param msg
     * @param t
     */
    protected static void fatal(String msg, Throwable t)
    {
        System.out.println(msg);
        if (t != null)
            t.printStackTrace();
        fail();
    }

    /**
     * Exit with a process failure code.
     */
    protected static void fail()
    {
        System.exit(1);
    }

    /**
     * Exit with a process success code.
     */
    protected static void succeed()
    {
        System.exit(0);
    }

    /**
     * Reads a character from stdin, blocks until it is not received.
     * 
     * @return true if use pressed `y`, false otherwise.
     */
    protected static boolean readYes() throws IOException
    {
        return (System.in.read() == 'y');
    }

    /**
     * Returns a value of a given Boolean object or false if the object is null.
     * 
     * @param bool Boolean object to check and return.
     * @return the value of a given Boolean object or false if the object is
     *         null.
     */
    protected static boolean getBoolOrFalse(Boolean bool)
    {
        if (bool != null)
            return bool;
        else
            return false;
    }

    /**
     * This class holds elements returned by the info query.
     * 
     * @see com.continuent.tungsten.replicator.thl.THLManagerCtrl#getInfo()
     */
    public static class InfoHolder
    {
        private long minSeqNo               = -1;
        private long maxSeqNo               = -1;
        private long eventCount             = -1;
        private long highestReplicatedEvent = -1;

        public InfoHolder(long minSeqNo, long maxSeqNo, long eventCount,
                long highestReplicatedEvent)
        {
            this.minSeqNo = minSeqNo;
            this.maxSeqNo = maxSeqNo;
            this.eventCount = eventCount;
            this.highestReplicatedEvent = highestReplicatedEvent;
        }

        public long getMinSeqNo()
        {
            return minSeqNo;
        }

        public long getMaxSeqNo()
        {
            return maxSeqNo;
        }

        public long getEventCount()
        {
            return eventCount;
        }

        public long getHighestReplicatedEvent()
        {
            return highestReplicatedEvent;
        }
    }
}
