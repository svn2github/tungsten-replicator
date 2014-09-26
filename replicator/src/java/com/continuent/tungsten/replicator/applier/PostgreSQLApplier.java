/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2008 Continuent Inc.
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

package com.continuent.tungsten.replicator.applier;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnSpec;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnVal;
import com.continuent.tungsten.replicator.extractor.mysql.SerialBlob;

public class PostgreSQLApplier extends JdbcApplier
{
    protected String      host   = "localhost";
    protected int         port   = 5432;

    public void setHost(String host)
    {
        this.host = host;
    }

    public void setPort(String portAsString)
    {
        this.port = Integer.parseInt(portAsString);
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
        if (columnSpec.getType() == Types.BLOB && columnSpec.isBlob())
        {
            // Handle data as blob only if a blob is expected
            SerialBlob blob = (SerialBlob) value.getValue();
            prepStatement.setBytes(bindLoc,
                    blob.getBytes(1, (int) blob.length()));
        }
        else
            prepStatement.setObject(bindLoc, value.getValue());
    }
}