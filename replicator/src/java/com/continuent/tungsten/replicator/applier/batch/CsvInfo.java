/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2011-12 Continuent Inc.
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

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import com.continuent.tungsten.common.csv.CsvWriter;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.database.Column;
import com.continuent.tungsten.replicator.database.Table;

/**
 * Defines a struct to hold batch CSV file information. When using staging the
 * stage table metadata field is filled in. Otherwise it is null.
 */
public class CsvInfo
{
    // Primary key of the table.
    protected String stagePkeyColumn;

    // Struct fields.
    public String    schema;
    public String    table;
    public Table     baseTableMetadata;
    public Table     stageTableMetadata;
    public File      file;
    public CsvWriter writer;

    /**
     * Instantiates a new instance.
     * 
     * @param stagePkeyColumn Name of the primary key column
     */
    public CsvInfo(String stagePkeyColumn)
    {
        this.stagePkeyColumn = stagePkeyColumn;
    }

    /**
     * Returns SQL substitution parameters for this CSV file.
     */
    public Map<String, String> getSqlParameters() throws ReplicatorException
    {
        // Generate data for base and staging tables.
        String pkey = getPKColumn();
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
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("%%CSV_FILE%%", file.getAbsolutePath());
        parameters
                .put("%%BASE_TABLE%%", baseTableMetadata.fullyQualifiedName());
        parameters.put("%%BASE_COLUMNS%%", colNames.toString());
        parameters.put("%%STAGE_TABLE%%",
                stageTableMetadata.getName());
        parameters.put("%%STAGE_SCHEMA%%",
                stageTableMetadata.getSchema());
        parameters.put("%%STAGE_TABLE_FQN%%",
                stageTableMetadata.fullyQualifiedName());
        parameters.put("%%PKEY%%", pkey);
        parameters.put("%%BASE_PKEY%%", basePkey);
        parameters.put("%%STAGE_PKEY%%", stagePkey);

        // Return parameters.
        return parameters;
    }

    /**
     * Determines primary key name for the given CsvInfo object. If underlying
     * meta data table contains a primary key, it is used. If not, user's
     * configured default one is taken.<br/>
     * Currently, only single-column primary keys are supported.
     * 
     * @return Primary key column name.
     * @throws ReplicatorException Thrown if primary key cannot be found
     */
    public String getPKColumn() throws ReplicatorException
    {
        String pkey = stagePkeyColumn;

        // If THL event contains PK, use it.
        if (baseTableMetadata.getPrimaryKey() != null
                && baseTableMetadata.getPrimaryKey().getColumns() != null
                && baseTableMetadata.getPrimaryKey().getColumns().size() > 0
                && baseTableMetadata.getPrimaryKey().getColumns().get(0)
                        .getName() != null
                && !baseTableMetadata.getPrimaryKey().getColumns().get(0)
                        .getName().equals(""))
        {
            pkey = baseTableMetadata.getPrimaryKey().getColumns().get(0)
                    .getName();
        }

        // If the primary key is still null that means we don't have a key
        // from metadata and nothing was set in the configuration properties.
        if (pkey == null)
        {
            String msg = String
                    .format("Unable to find a primary key for %s and there is no default from property stagePkeyColumn",
                            baseTableMetadata.fullyQualifiedName());
            throw new ReplicatorException(msg);
        }

        return pkey;
    }
}
