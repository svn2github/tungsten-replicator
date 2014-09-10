
package com.continuent.tungsten.common.patterns.order;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.cluster.resource.DataSource;
import com.continuent.tungsten.common.cluster.resource.DatabaseVendors;
import com.continuent.tungsten.common.utils.CLUtils;

public class HighWaterResourceFactory
{
    private static Logger logger                  = Logger.getLogger(HighWaterResource.class);
    static int            binlogPositionMaxLength = 0;

    public static HighWaterResource getHighWater(DataSource ds,
            Connection conn, String replicationSchemaName) throws SQLException
    {
        if (ds.getVendor() == null)
        {
            throw new SQLException(String.format(
                    "Vendor not set in datasource %s, unable to get highwater",
                    ds.getName()));
        }

        if (ds.getVendor().equals(DatabaseVendors.MYSQL))
        {
            return mysqlHighWater(ds, conn, replicationSchemaName);
        }
        else
        {
            throw new SQLException(
                    String.format(
                            "Cannot get highwater for vendor = '%s' - functionality is not implemented",
                            ds.getVendor()));
        }
    }

    private static HighWaterResource mysqlHighWater(DataSource ds,
            Connection conn, String replicationSchemaName) throws SQLException
    {

        if (conn == null)
        {
            throw new SQLException("Connection is null");
        }
        else if (conn.isClosed())
        {
            throw new SQLException("connection is closed");
        }

        if (binlogPositionMaxLength == 0)
        {
            getMaxBinlogSize(conn);
        }

        String queryToExecute = String
                .format("SHOW MASTER STATUS;select epoch_number from %s.trep_commit_seqno",
                        replicationSchemaName);

        ResultSet result = null;
        Statement stmt = conn.createStatement();

        try
        {
            result = stmt.executeQuery(queryToExecute);

            if (!result.next())
            {
                throw new SQLException(
                        "Unable to get master status; is the MySQL binlog enabled?");
            }

            /*
             * We need to process 2 result sets. The first one will be the
             * master status and the second the epoch_number from
             * trep_commit_seqno.
             */

            String eventId = null;
            long highWaterEpoch = -1;

            for (int resultSetId = 0; resultSetId < 2; resultSetId++)
            {
                if (resultSetId == 0)
                {
                    String binlogFile = result.getString(1);
                    int binlogOffset = result.getInt(2);
                    String binlogOffsetAsString = String.format("%0"
                            + (binlogPositionMaxLength + 1) + "d", new Integer(
                            binlogOffset));

                    eventId = String.format("%s:%s", binlogFile,
                            binlogOffsetAsString);
                }
                else
                {
                    if (!stmt.getMoreResults())
                    {
                        throw new SQLException(
                                String.format(
                                        "Did not get a result set with the replication epoch number.\n"
                                                + "Executed query: %s\n"
                                                + "Be sure that the replication schema exists etc.",
                                        queryToExecute));
                    }

                    result = stmt.getResultSet();

                    if (!result.next())
                    {
                        throw new SQLException(
                                String.format(
                                        "Did not get a column set with the replication epoch number.\n"
                                                + "Executed query: %s\n"
                                                + "Be sure that the replication schema exists etc.",
                                        queryToExecute));
                    }

                    result = stmt.getResultSet();
                    highWaterEpoch = result.getLong(1);
                }
            }

            HighWaterResource highWater = new HighWaterResource(highWaterEpoch,
                    eventId);

            return highWater;
        }
        finally
        {
            cleanUpDatabaseResources(null, stmt, result);
        }
    }

    // Fetch mysql 'max_binlog_size' setting.
    private static void getMaxBinlogSize(Connection conn) throws SQLException
    {
        Statement st = null;
        ResultSet rs = null;
        try
        {
            st = conn.createStatement();
            rs = st.executeQuery("show variables like 'max_binlog_size'");
            if (rs.next())
            {
                binlogPositionMaxLength = rs.getString(1).length();
            }
        }
        catch (SQLException e)
        {
            String message = "Unable to connect to MySQL server to get max_binlog_size setting; is server available?";
            logger.error(message);
            throw e;
        }
        finally
        {
            cleanUpDatabaseResources(null, st, rs);
        }
    }

    // Utility method to close result, statement, and connection objects.
    private static void cleanUpDatabaseResources(Connection conn, Statement st,
            ResultSet rs) throws SQLException
    {
        if (rs != null)
        {
            try
            {
                rs.close();
            }
            catch (SQLException ignore)
            {
            }
        }
        if (st != null)
        {
            try
            {
                st.close();
            }
            catch (SQLException ignore)
            {
            }
        }
        if (conn != null)
            conn.close();
    }
}
