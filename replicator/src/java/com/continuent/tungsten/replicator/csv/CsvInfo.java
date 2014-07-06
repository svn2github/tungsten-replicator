/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2011-2014 Continuent Inc.
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

package com.continuent.tungsten.replicator.csv;

import java.io.File;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.database.Column;
import com.continuent.tungsten.replicator.database.Key;
import com.continuent.tungsten.replicator.database.Table;

/**
 * Defines a struct to hold batch CSV file information. When using staging the
 * stage table metadata field is filled in. Otherwise it is null.
 */
public class CsvInfo
{
    // Struct fields.
    public String schema;
    public String table;
    public String key;
    public Table  baseTableMetadata;
    public Table  stageTableMetadata;
    public File   file;
    public long   startSeqno = -1;
    public long   endSeqno   = -1;

    /**
     * Instantiates a new instance.
     */
    public CsvInfo()
    {
    }

    /**
     * Returns SQL substitution parameters for this CSV file.
     */
    public Map<String, Object> getSqlParameters() throws ReplicatorException
    {
        // Generate data for base and staging tables.
        List<String> pkey = getPKColumns();
        String basePkey = baseTableMetadata.getName() + "." + pkey;
        String stagePkey = stageTableMetadata.getName() + "." + pkey;
        StringBuffer colNames = new StringBuffer();
        for (Column col : baseTableMetadata.getAllColumns())
        {
            if (colNames.length() > 0)
                colNames.append(",");
            colNames.append(col.getName());
        }

        // Create map containing parameters.
        Map<String, Object> parameters = new HashMap<String, Object>();
        parameters.put("%%CSV_FILE%%", file.getAbsolutePath());
        parameters
                .put("%%BASE_TABLE%%", baseTableMetadata.fullyQualifiedName());
        parameters.put("%%BASE_COLUMNS%%", colNames.toString());
        parameters.put("%%STAGE_TABLE%%", stageTableMetadata.getName());
        parameters.put("%%STAGE_SCHEMA%%", stageTableMetadata.getSchema());
        parameters.put("%%STAGE_TABLE_FQN%%",
                stageTableMetadata.fullyQualifiedName());
        parameters.put("%%PKEY%%", pkey);
        parameters.put("%%BASE_PKEY%%", basePkey);
        parameters.put("%%STAGE_PKEY%%", stagePkey);

        // Return parameters.
        return parameters;
    }

    /**
     * Determines primary key names for the given CsvInfo object. If underlying
     * metadata table contains a primary key, it is used.
     * 
     * @return Primary key column names.
     * @throws ReplicatorException Thrown if primary key cannot be found
     */
    public List<String> getPKColumns() throws ReplicatorException
    {
        LinkedList<String> primaryKeyColumns = new LinkedList<String>();

        // If THL event contains PK, use it.
        if (baseTableMetadata.getPrimaryKey() != null
                && baseTableMetadata.getPrimaryKey().getColumns() != null)
        {
            Key pkey = baseTableMetadata.getPrimaryKey();
            for (Column pkCol : pkey.getColumns())
            {
                String name = pkCol.getName();
                if (name != null && !"".equals(name))
                    primaryKeyColumns.add(name);
            }
        }
        return primaryKeyColumns;
    }
}
