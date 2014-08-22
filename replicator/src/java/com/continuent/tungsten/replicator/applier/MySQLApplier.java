/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2014 Continuent Inc.
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
 * Initial developer(s): Teemu Ollakka
 * Contributor(s):Stephane Giron
 */

package com.continuent.tungsten.replicator.applier;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.database.Column;
import com.continuent.tungsten.replicator.database.Table;
import com.continuent.tungsten.replicator.datatypes.MySQLUnsignedNumeric;
import com.continuent.tungsten.replicator.datatypes.Numeric;
import com.continuent.tungsten.replicator.dbms.LoadDataFileQuery;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnSpec;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnVal;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.dbms.RowIdData;
import com.continuent.tungsten.replicator.event.ReplOption;
import com.continuent.tungsten.replicator.extractor.mysql.SerialBlob;

/**
 * Stub applier class that automatically constructs url from Oracle-specific
 * properties like host, port, and service.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class MySQLApplier extends JdbcApplier
{
    private static Logger logger     = Logger.getLogger(MySQLApplier.class);

    protected String      host       = "localhost";
    protected int         port       = 3306;
    protected String      urlOptions = null;

    /**
     * Host name or IP address.
     */
    public void setHost(String host)
    {
        this.host = host;
    }

    /**
     * TCP/IP port number, a positive integer.
     */
    public void setPort(String portAsString)
    {
        this.port = Integer.parseInt(portAsString);
    }

    /**
     * JDBC URL options with a leading ?.
     */
    public void setUrlOptions(String urlOptions)
    {
        this.urlOptions = urlOptions;
    }

    protected void applyRowIdData(RowIdData data) throws ReplicatorException
    {
        String query = "SET ";

        switch (data.getType())
        {
            case RowIdData.LAST_INSERT_ID :
                query += "LAST_INSERT_ID";
                break;
            case RowIdData.INSERT_ID :
                query += "INSERT_ID";
                break;
            default :
                // Old behavior
                query += "INSERT_ID";
                break;
        }
        query += " = " + data.getRowId();

        try
        {
            try
            {
                statement.execute(query);
            }
            catch (SQLWarning e)
            {
                String msg = "While applying SQL event:\n" + data.toString()
                        + "\nWarning: " + e.getMessage();
                logger.warn(msg);
            }
            statement.clearBatch();

            if (logger.isDebugEnabled())
            {
                logger.debug("Applied event: " + query);
            }
        }
        catch (SQLException e)
        {
            logFailedStatementSQL(query, e);
            throw new ApplierException(e);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.JdbcApplier#addColumn(java.sql.ResultSet,
     *      java.lang.String)
     */
    @Override
    protected Column addColumn(ResultSet rs, String columnName)
            throws SQLException
    {
        String typeDesc = rs.getString("TYPE_NAME").toUpperCase();
        boolean isSigned = !typeDesc.contains("UNSIGNED");
        int dataType = rs.getInt("DATA_TYPE");

        if (logger.isDebugEnabled())
            logger.debug("Adding column " + columnName + " (TYPE " + dataType
                    + " - " + (isSigned ? "SIGNED" : "UNSIGNED") + ")");

        Column column = new Column(columnName, dataType, false, isSigned);
        column.setTypeDescription(typeDesc);
        column.setLength(rs.getLong("column_size"));

        return column;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.JdbcApplier#setObject(java.sql.PreparedStatement,
     *      int, com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnVal,
     *      com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnSpec)
     */
    @Override
    protected void setObject(PreparedStatement prepStatement, int bindLoc,
            ColumnVal value, ColumnSpec columnSpec) throws SQLException
    {

        int type = columnSpec.getType();

        if (type == Types.TIMESTAMP && value.getValue() instanceof Integer)
        {
            prepStatement.setInt(bindLoc, 0);
        }
        else if (type == Types.DATE && value.getValue() instanceof Integer)
        {
            prepStatement.setInt(bindLoc, 0);
        }
        else if (type == Types.INTEGER)
        {
            Object valToInsert = null;
            Numeric numeric = new Numeric(columnSpec, value);
            if (columnSpec.isUnsigned() && numeric.isNegative())
            {
                valToInsert = MySQLUnsignedNumeric
                        .negativeToMeaningful(numeric);
                setInteger(prepStatement, bindLoc, valToInsert);
            }
            else
                prepStatement.setObject(bindLoc, value.getValue());
        }
        else if (type == java.sql.Types.BLOB
                && value.getValue() instanceof SerialBlob)
        {
            SerialBlob val = (SerialBlob) value.getValue();
            prepStatement
                    .setBytes(bindLoc, val.getBytes(1, (int) val.length()));
        }
        else
            prepStatement.setObject(bindLoc, value.getValue());
    }

    protected void setInteger(PreparedStatement prepStatement, int bindLoc,
            Object valToInsert) throws SQLException
    {
        prepStatement.setObject(bindLoc, valToInsert);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.JdbcApplier#applyStatementData(com.continuent.tungsten.replicator.dbms.StatementData)
     */
    @Override
    protected void applyLoadDataLocal(LoadDataFileQuery data, File temporaryFile)
            throws ReplicatorException
    {
        try
        {
            int[] updateCount;
            String schema = data.getDefaultSchema();
            Long timestamp = data.getTimestamp();
            List<ReplOption> options = data.getOptions();

            applyUseSchema(schema);

            applySetTimestamp(timestamp);

            applySessionVariables(options);

            try
            {
                updateCount = statement.executeBatch();
            }
            catch (SQLWarning e)
            {
                String msg = "While applying SQL event:\n" + data.toString()
                        + "\nWarning: " + e.getMessage();
                logger.warn(msg);
                updateCount = new int[1];
                updateCount[0] = statement.getUpdateCount();
            }
            statement.clearBatch();
        }
        catch (SQLException e)
        {
            logFailedStatementSQL(data.getQuery(), e);
            throw new ApplierException(e);
        }

        try
        {
            FileInputStream fis = new FileInputStream(temporaryFile);
            ((com.mysql.jdbc.Statement) statement)
                    .setLocalInfileInputStream(fis);

            int cnt = statement.executeUpdate(data.getQuery());

            if (logger.isDebugEnabled())
                logger.debug("Applied event (update count " + cnt + "): "
                        + data.toString());
        }
        catch (SQLException e)
        {
            logFailedStatementSQL(data.getQuery(), e);
            throw new ApplierException(e);
        }
        catch (FileNotFoundException e)
        {
            logFailedStatementSQL(data.getQuery());
            throw new ApplierException(e);
        }
        finally
        {
            ((com.mysql.jdbc.Statement) statement)
                    .setLocalInfileInputStream(null);
        }

        // Clean up the temp file as we may not get a delete file event.
        if (logger.isDebugEnabled())
        {
            logger.debug("Deleting temp file: "
                    + temporaryFile.getAbsolutePath());
        }
        temporaryFile.delete();
    }

    private static final char[] hexArray = "0123456789abcdef".toCharArray();

    protected String hexdump(byte[] buffer)
    {
        char[] hexChars = new char[buffer.length * 2];
        for (int j = 0; j < buffer.length; j++)
        {
            int v = buffer[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.JdbcApplier#applyOneRowChangePrepared(com.continuent.tungsten.replicator.dbms.OneRowChange)
     */
    @Override
    protected void applyOneRowChangePrepared(OneRowChange oneRowChange)
            throws ReplicatorException
    {
        // TODO : Optimize events when number of rows is > min or < to max ?
        if (optimizeRowEvents)
            if (oneRowChange.getAction() == RowChangeData.ActionType.INSERT
                    && oneRowChange.getColumnValues().size() > 1)
            {
                // optimize inserts
                getColumnInfomation(oneRowChange);

                executePreparedStatement(oneRowChange,
                        prepareOptimizedInsertStatement(oneRowChange),
                        oneRowChange.getColumnSpec(),
                        oneRowChange.getColumnValues());
                return;
            }
            else if (oneRowChange.getAction() == RowChangeData.ActionType.DELETE
                    && oneRowChange.getKeyValues().size() > 1)
            {
                getColumnInfomation(oneRowChange);

                Table t = null;

                try
                {
                    t = getTableMetadata(oneRowChange);
                }
                catch (SQLException e)
                {
                    throw new ApplierException(
                            "Failed to retrieve table metadata from database",
                            e);
                }

                // This can only be applied if table has a single column primary
                // key
                // TODO : Some datatypes might need to be excluded
                if (t.getPrimaryKey() != null
                        && t.getPrimaryKey().getColumns() != null
                        && t.getPrimaryKey().getColumns().size() == 1)
                {
                    String keyName = t.getPrimaryKey().getColumns().get(0)
                            .getName();

                    executePreparedStatement(
                            oneRowChange,
                            prepareOptimizedDeleteStatement(oneRowChange,
                                    keyName), oneRowChange.getKeySpec(),
                            oneRowChange.getKeyValues());
                    return;
                }
                else if (logger.isDebugEnabled())
                    logger.debug("Unable to optimize delete statement as no suitable primary key was found for : "
                            + oneRowChange.getSchemaName()
                            + "."
                            + oneRowChange.getTableName());
            }
        // No optimization found, let's run the unoptimized statement form.
        super.applyOneRowChangePrepared(oneRowChange);
    }

    /**
     * Build prepare statement for optimized inserts : <br>
     * INSERT INTO table1 VALUES (...) ; INSERT INTO table1 VALUES (...) ; ...
     * would translate into<br>
     * INSERT INTO table1 VALUES (...), (...), ...
     * 
     * @param oneRowChange row event being processed
     * @return
     */
    private StringBuffer prepareOptimizedInsertStatement(
            OneRowChange oneRowChange)
    {
        StringBuffer stmt;
        stmt = new StringBuffer();
        stmt.append("INSERT INTO ");
        stmt.append(conn.getDatabaseObjectName(oneRowChange.getSchemaName())
                + "." + conn.getDatabaseObjectName(oneRowChange.getTableName()));
        stmt.append(" ( ");
        printColumnSpec(stmt, oneRowChange.getColumnSpec(), null, null,
                PrintMode.NAMES_ONLY, ", ");
        stmt.append(") VALUES (");

        boolean firstRow = true;
        for (ArrayList<ColumnVal> oneRowValues : oneRowChange.getColumnValues())
        {
            if (firstRow)
            {
                firstRow = false;
            }
            else
                stmt.append(", (");

            printColumnSpec(stmt, oneRowChange.getColumnSpec(), null,
                    oneRowValues, PrintMode.PLACE_HOLDER, " , ");

            stmt.append(")");
        }
        return stmt;
    }

    /**
     * TODO: prepareOptimizedDeleteStatement definition.
     * 
     * @param oneRowChange
     * @param keyName
     * @return
     */
    private StringBuffer prepareOptimizedDeleteStatement(
            OneRowChange oneRowChange, String keyName)
    {
        StringBuffer stmt = new StringBuffer();
        stmt.append("DELETE FROM ");
        stmt.append(conn.getDatabaseObjectName(oneRowChange.getSchemaName())
                + "." + conn.getDatabaseObjectName(oneRowChange.getTableName()));
        stmt.append(" WHERE ");
        stmt.append(conn.getDatabaseObjectName(keyName));
        stmt.append(" IN (");

        ArrayList<ArrayList<ColumnVal>> values = oneRowChange.getKeyValues();
        ArrayList<ColumnSpec> keySpec = oneRowChange.getKeySpec();

        boolean firstRow = true;
        for (ArrayList<ColumnVal> oneKeyValues : values)
        {
            if (firstRow)
                firstRow = false;
            else
                stmt.append(", ");

            printColumnSpec(stmt, keySpec, null, oneKeyValues,
                    PrintMode.PLACE_HOLDER, " , ");
        }
        stmt.append(")");
        return stmt;
    }

    /**
     * TODO: executePreparedStatement definition.
     * 
     * @param oneRowChange
     * @param stmt
     * @param spec
     * @param values
     * @throws ApplierException
     */
    private void executePreparedStatement(OneRowChange oneRowChange,
            StringBuffer stmt, ArrayList<ColumnSpec> spec,
            ArrayList<ArrayList<ColumnVal>> values) throws ApplierException
    {
        PreparedStatement prepStatement = null;
        try
        {
            String statement = stmt.toString();
            if (logger.isDebugEnabled())
                logger.debug("Statement is "
                        + statement.substring(1,
                                Math.min(statement.length(), 500)));
            prepStatement = conn.prepareStatement(statement);
            int bindLoc = 1; /* Start binding at index 1 */

            for (ArrayList<ColumnVal> oneRowValues : values)
            {
                bindLoc = bindColumnValues(prepStatement, oneRowValues,
                        bindLoc, spec, false);

            }

            try
            {
                prepStatement.executeUpdate();
            }
            catch (SQLWarning e)
            {
                String msg = "While applying SQL event:\n" + statement
                        + "\nWarning: " + e.getMessage();
                logger.warn(msg);
            }

            // if (logger.isDebugEnabled())
            // {
            // logger.debug("Applied event (update count " + updateCount
            // + "): " + stmt.toString());
            // }

        }
        catch (SQLException e)
        {
            ApplierException applierException = new ApplierException(e);
            applierException.setExtraData(logFailedRowChangeSQL(stmt,
                    oneRowChange));
            throw applierException;
        }
        finally
        {
            if (prepStatement != null)
                try
                {
                    prepStatement.close();
                }
                catch (SQLException e)
                {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
        }
    }
}