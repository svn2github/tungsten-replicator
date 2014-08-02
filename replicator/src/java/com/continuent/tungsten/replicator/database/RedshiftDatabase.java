/**
 * Tungsten: An Application Server for uni/cluster.
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
 * Initial developer(s): Linas Virbalas
 * Contributor(s): 
 */

package com.continuent.tungsten.replicator.database;

import java.sql.SQLException;
import java.sql.Types;

import org.apache.log4j.Logger;

/**
 * Implements DBMS-specific operations for Amazon Redshift.
 * 
 * @author <a href="mailto:linas.virbalas@continuent.com">Linas Virbalas</a>
 */
public class RedshiftDatabase extends PostgreSQLDatabase
{
    private static Logger logger = Logger.getLogger(RedshiftDatabase.class);

    public RedshiftDatabase() throws SQLException
    {
        dbms = DBMS.REDSHIFT;
        // Redshift uses PostgreSQL driver.
        dbDriver = "org.postgresql.Driver";
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.Database#dropTable(com.continuent.tungsten.replicator.database.Table)
     */
    @Override
    public void dropTable(Table table)
    {
        // Temporary tables cannot specify a schema name:
        String SQL = "DROP TABLE " + table.getSchema()
                + (table.isTemporary() ? "_" : ".") + table.getName() + " ";

        try
        {
            execute(SQL);
        }
        catch (SQLException e)
        {
            if (logger.isDebugEnabled())
                logger.debug("Unable to drop table; this may be expected", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    public boolean supportsBLOB()
    {
        return false;
    }

    /**
     * Converts column types according to standard Vertica names.
     */
    protected String columnToTypeString(Column c, String tableType)
    {
        switch (c.getType())
        {
            case Types.TINYINT :
                return "SMALLINT";

            case Types.SMALLINT :
                return "SMALLINT";

            case Types.INTEGER :
                return "INTEGER";

            case Types.BIGINT :
                return "BIGINT";

            case Types.BOOLEAN :
                return "BOOLEAN";

            case Types.CHAR :
            {
                if (c.getLength() == 1)
                    // TODO: remove this dirty hack and fix it in the callers.
                    // Historically, as MySQL doesn't have a BOOLEAN type,
                    // callers create tables with CHAR(1) instead (though then
                    // use set/getBoolean), but having a CHAR and then use
                    // set/getBoolean won't work for Redshift, hence we change
                    // it to correct type here.
                    return "BOOLEAN";
                else
                    return "CHAR(" + c.getLength() + ")";
            }

            case Types.VARCHAR :
                return "VARCHAR(" + c.getLength() + ")";

            case Types.DATE :
                return "DATE";

            case Types.TIMESTAMP :
                return "TIMESTAMP";

            case Types.CLOB :
                // Redshift does not support TEXT directly, so we use biggest
                // allowed varchar.
                return "VARCHAR(65535)";

            default :
                return "UNKNOWN";
        }
    }
}
