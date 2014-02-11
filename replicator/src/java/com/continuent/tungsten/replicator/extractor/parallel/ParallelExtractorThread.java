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
 * Initial developer(s): Stephane Giron
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.extractor.parallel;

import java.io.Serializable;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

import oracle.sql.TIMESTAMPTZ;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.database.Column;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.DatabaseFactory;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnSpec;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnVal;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.event.DBMSEmptyEvent;
import com.continuent.tungsten.replicator.event.DBMSEvent;

/**
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class ParallelExtractorThread extends Thread
{
    private static Logger                 logger     = Logger.getLogger(ParallelExtractorThread.class);

    private String                        url;
    private String                        user;
    private String                        password;

    private Database                      connection = null;
    private boolean                       cancelled  = false;
    private ArrayBlockingQueue<DBMSEvent> queue;
    private ArrayBlockingQueue<Chunk>     chunks;

    // TODO : do we need 2 different notions for chunk size (the size of the
    // select) and rowcount (the maximum number of rows of the event) ?
    private int                           rowCount   = 10000;

    public ParallelExtractorThread(String url, String user, String password,
            ArrayBlockingQueue<Chunk> chunks,
            ArrayBlockingQueue<DBMSEvent> queue)
    {
        this.user = user;
        this.password = password;
        this.url = url;

        try
        {
            connection = DatabaseFactory.createDatabase(url, user, password);
        }
        catch (SQLException e)
        {
        }

        this.queue = queue;
        this.chunks = chunks;

    }

    public void prepare() throws ReplicatorException
    {
        try
        {
            connection = DatabaseFactory.createDatabase(url, user, password);
        }
        catch (SQLException e)
        {
        }

    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Thread#run()
     */
    @Override
    public void run()
    {
        runTask();
    }

    private void runTask()
    {
        String sqlString = null;
        try
        {
            connection.connect();
        }
        catch (SQLException e)
        {
            // throw new ReplicatorException("Unable to connect to Oracle", e);
        }

        Statement stmt = null;
        try
        {
            stmt = connection.createStatement();
        }
        catch (SQLException e1)
        {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        while (!cancelled)
        {
            // 1. get a table to process
            Chunk chunk;
            try
            {
                chunk = chunks.take();
            }
            catch (InterruptedException e1)
            {
                continue;
            }

            // 2. Read the table content and generate events
            if (chunk.getTable() == null)
            {
                logger.info("No more table found ... Exiting.");
                // Work complete : exit the loop
                try
                {
                    queue.put(new DBMSEmptyEvent(null));
                }
                catch (InterruptedException e)
                {
                }
                return;
            }

            // 2.1. Build the statement
            sqlString = buildSQLStatement(chunk);

            ArrayList<Column> allColumns = chunk.getTable().getAllColumns();
            ArrayList<DBMSData> dataArray = new ArrayList<DBMSData>();

            ResultSet rs = null;
            try
            {
                if (logger.isDebugEnabled())
                    logger.debug("Thread " + this.getName() + " running : "
                            + sqlString);
                rs = stmt.executeQuery(sqlString);
                if (rs != null)
                {
                    boolean eventSent;

                    if (rs.next())
                    {
                        RowChangeData rowChangeData = new RowChangeData();
                        dataArray.add(rowChangeData);

                        int rowIndex = 0;
                        OneRowChange oneRowChange = new OneRowChange();
                        rowChangeData.appendOneRowChange(oneRowChange);

                        ArrayList<ArrayList<OneRowChange.ColumnVal>> rows = oneRowChange
                                .getColumnValues();

                        do
                        {
                            eventSent = false;
                            rows.add(new ArrayList<ColumnVal>());

                            if (oneRowChange.getAction() == null)
                            {
                                oneRowChange.setSchemaName(chunk.getTable()
                                        .getSchema());
                                oneRowChange.setTableName(chunk.getTable()
                                        .getName());
                                oneRowChange
                                        .setAction(RowChangeData.ActionType.INSERT);
                            }

                            String columnName = null;

                            for (Column column : allColumns)
                            {
                                columnName = column.getName();

                                if (chunk.getColumns() != null
                                        && !chunk.getColumns().contains(
                                                columnName))
                                    // A list of columns is provided but the
                                    // column which is currently handled does
                                    // not belong to this set
                                    continue;

                                if (rowIndex == 0)
                                {
                                    OneRowChange.ColumnSpec spec = oneRowChange.new ColumnSpec();
                                    spec.setIndex(column.getPosition());
                                    spec.setName(column.getName());

                                    if (chunk.getColumns() != null)
                                        setTypeFromDatabase(
                                                column,
                                                spec,
                                                rs.getMetaData(),
                                                chunk.getColumns().indexOf(
                                                        columnName) + 1);
                                    else
                                        setTypeFromDatabase(column, spec,
                                                rs.getMetaData());

                                    spec.setLength((int) column.getLength());
                                    oneRowChange.getColumnSpec().add(spec);
                                }

                                OneRowChange.ColumnVal value = oneRowChange.new ColumnVal();
                                rows.get(rowIndex).add(value);

                                Object val = null;

                                int columnType = column.getType();
                                switch (columnType)
                                {
                                    case Types.DATE :
                                        String dateVal = rs
                                                .getString(columnName);
                                        if (dateVal != null
                                                && dateVal.equals("0000-00-00"))
                                            val = Integer.valueOf(0);
                                        else
                                            val = rs.getDate(columnName);
                                        break;
                                    case Types.TIMESTAMP :
                                        String timestampVal = rs
                                                .getString(columnName);
                                        if (timestampVal != null
                                                && timestampVal
                                                        .equals("0000-00-00 00:00:00"))
                                            val = Integer.valueOf(0);
                                        else
                                            val = rs.getTimestamp(columnName);
                                        break;
                                    case Types.BIGINT :
                                        Object bigintVal = rs
                                                .getObject(columnName);
                                        if (bigintVal != null)
                                            val = Long
                                                    .valueOf(((BigInteger) bigintVal)
                                                            .longValue());
                                        break;
                                    case oracle.jdbc.OracleTypes.TIMESTAMPTZ :
                                        Object object = rs
                                                .getObject(columnName);
                                        if (object instanceof TIMESTAMPTZ)
                                        {
                                            TIMESTAMPTZ timestampTZ = (TIMESTAMPTZ) object;
                                            value.setValue(TIMESTAMPTZ.toTimestamp(
                                                    connection.getConnection(),
                                                    timestampTZ.getBytes()));
                                        }
                                        break;
                                    default :
                                        val = rs.getObject(column.getName());
                                        break;
                                }
                                if (rs.wasNull())
                                    value.setValueNull();
                                else
                                    value.setValue((Serializable) val);
                            }
                            rowIndex++;

                            if (rowIndex >= rowCount)
                            {
                                eventSent = true;
                                try
                                {
                                    DBMSEvent ev = new DBMSEvent(
                                            "PROVISIONNING", dataArray,
                                            new Timestamp(
                                                    System.currentTimeMillis()));
                                    ev.addMetadataOption("schema", chunk
                                            .getTable().getSchema());
                                    ev.addMetadataOption("table", chunk
                                            .getTable().getName());
                                    ev.addMetadataOption("nbBlocks",
                                            String.valueOf(chunk.getNbBlocks()));
                                    queue.put(ev);
                                }
                                catch (InterruptedException e)
                                {
                                    e.printStackTrace();
                                }
                                rowIndex = 0;
                                dataArray = new ArrayList<DBMSData>();
                                rowChangeData = new RowChangeData();
                                dataArray.add(rowChangeData);

                                oneRowChange = new OneRowChange();
                                rows = oneRowChange.getColumnValues();
                                rowChangeData.appendOneRowChange(oneRowChange);
                            }
                        }
                        while (rs.next());

                        if (!eventSent)
                        {
                            try

                            {
                                // TODO: what should be the event id ?
                                DBMSEvent ev = new DBMSEvent("PROVISIONNING",
                                        dataArray, new Timestamp(
                                                System.currentTimeMillis()));
                                ev.addMetadataOption("schema", chunk.getTable()
                                        .getSchema());
                                ev.addMetadataOption("table", chunk.getTable()
                                        .getName());
                                ev.addMetadataOption("nbBlocks",
                                        String.valueOf(chunk.getNbBlocks()));
                                queue.put(ev);
                            }
                            catch (InterruptedException e)
                            {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                            }
                        }
                    }
                    else
                    // nothing more
                    {
                    }
                }
            }
            catch (SQLException e)
            {
                logger.error("SQL failed : " + sqlString, e);
            }
            finally
            {
                if (rs != null)
                    try
                    {
                        rs.close();
                    }
                    catch (SQLException e)
                    {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
            }
            // 3. Get to next available table, if any
        }
    }

    private String buildSQLStatement(Chunk chunk)
    {
        if (logger.isDebugEnabled())
            logger.debug("Got chunk for " + chunk.getTable() + " from "
                    + chunk.getFrom() + " to " + chunk.getTo());

        StringBuffer sql = new StringBuffer();

        List<String> columns = chunk.getColumns();
        if (columns == null)
            for (Column column : chunk.getTable().getAllColumns())
            {
                if (sql.length() == 0)
                {
                    sql.append("SELECT ");
                }
                else
                {
                    sql.append(", ");
                }
                sql.append(column.getName());
            }
        else
            for (Iterator<String> iterator = columns.iterator(); iterator
                    .hasNext();)
            {
                if (sql.length() == 0)
                {
                    sql.append("SELECT ");
                }
                else
                {
                    sql.append(", ");
                }
                sql.append(iterator.next());
            }

        sql.append(" FROM ");
        sql.append(connection.getDatabaseObjectName(chunk.getTable()
                .getSchema()));
        sql.append('.');
        sql.append(connection.getDatabaseObjectName(chunk.getTable().getName()));

        String pkName = chunk.getTable().getPrimaryKey().getColumns().get(0)
                .getName();

        if (chunk instanceof NumericChunk && ((Long) chunk.getFrom()) > 0)
        {

            sql.append(" WHERE ");
            sql.append(pkName);
            sql.append(" > ");
            sql.append(chunk.getFrom());
            sql.append(" AND ");
            sql.append(pkName);
            sql.append(" <= ");
            sql.append(chunk.getTo());
        }
        else if (chunk instanceof StringChunk
                && ((String) chunk.getFrom()).length() > 0)
        {
            sql.append(" WHERE ");
            sql.append(pkName);
            sql.append(" >= '");
            sql.append(chunk.getFrom());
            sql.append("' AND ");
            sql.append(pkName);
            sql.append(" <= '");
            sql.append(chunk.getTo());
            sql.append("'");
        }

        return sql.toString();
    }

    private void setTypeFromDatabase(Column column, ColumnSpec spec,
            ResultSetMetaData metaData)
    {
        setTypeFromDatabase(column, spec, metaData, column.getPosition());
    }

    /**
     * TODO: setTypeFromDatabase definition.
     * 
     * @param column
     * @param spec
     * @param resultSetMetaData
     * @param position
     */
    private void setTypeFromDatabase(Column column,
            OneRowChange.ColumnSpec spec, ResultSetMetaData resultSetMetaData,
            int position)
    {
        try
        {
            column.setType(resultSetMetaData.getColumnType(position));
        }
        catch (SQLException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        switch (column.getType())
        {
            case Types.BIGINT :
                spec.setLength(8);
                spec.setType(Types.INTEGER);
                break;

            case Types.INTEGER :
            case Types.TINYINT :
            case Types.SMALLINT :
                spec.setLength(4);
                spec.setType(Types.INTEGER);
                break;

            default :
                spec.setType(column.getType());
                spec.setLength((int) column.getLength());
                break;
        }
    }

    public void cancel()
    {
        this.cancelled = true;

        if (connection != null)
        {
            connection.close();
            connection = null;
        }

    }

}
