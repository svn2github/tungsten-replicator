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
    // Struct fields. These are public to simplify access from
    // Javascript.
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
     * Returns SQL substitution parameters for this CSV file. These are somewhat
     * deprecated as merge scripts can query this instance directly using
     * Javascript.
     */
    public Map<String, Object> getSqlParameters() throws ReplicatorException
    {
        // Generate data for base and staging tables.
        List<String> pkey = getPKColumns();
        String basePkey = baseTableMetadata.getName() + "." + pkey;
        String stagePkey = stageTableMetadata.getName() + "." + pkey;

        // Create map containing parameters.
        Map<String, Object> parameters = new HashMap<String, Object>();
        parameters.put("%%CSV_FILE%%", file.getAbsolutePath());
        parameters.put("%%BASE_TABLE%%", this.getBaseTableFQN());
        parameters.put("%%BASE_COLUMNS%%", this.getBaseColumnList());
        parameters.put("%%STAGE_TABLE%%", stageTableMetadata.getName());
        parameters.put("%%STAGE_SCHEMA%%", stageTableMetadata.getSchema());
        parameters.put("%%STAGE_TABLE_FQN%%", this.getStageTableFQN());
        parameters.put("%%PKEY%%", pkey);
        parameters.put("%%BASE_PKEY%%", basePkey);
        parameters.put("%%STAGE_PKEY%%", stagePkey);

        // Return parameters.
        return parameters;
    }

    /**
     * Returns the fully qualified name of the base table.
     */
    public String getBaseTableFQN()
    {
        return baseTableMetadata.fullyQualifiedName();
    }

    /**
     * Returns the fully qualified name of the staging table.
     */
    public String getStageTableFQN()
    {
        return stageTableMetadata.fullyQualifiedName();
    }

    /**
     * Returns the base table column names as a comma-separated list suitable
     * for use in SQL commands.
     */
    public String getBaseColumnList()
    {
        StringBuffer colNames = new StringBuffer();
        for (Column col : baseTableMetadata.getAllColumns())
        {
            if (colNames.length() > 0)
                colNames.append(",");
            colNames.append(col.getName());
        }
        return colNames.toString();
    }

    /**
     * Determines primary key names for the given CsvInfo object. If underlying
     * metadata table contains a primary key, it is used.
     * 
     * @return Primary key column names.
     */
    public List<String> getPKColumns()
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

    /**
     * Returns a comma separated list of primary key columns.
     */
    public String getPKColumnList()
    {
        StringBuffer keyColList = new StringBuffer();
        List<String> pkeys = this.getPKColumns();
        for (int i = 0; i < pkeys.size(); i++)
        {
            String pkey = pkeys.get(i);
            if (i > 0)
                keyColList.append(",");
            keyColList.append(pkey);
        }
        return keyColList.toString();
    }

    /**
     * Returns a list of joined keys suitable for plunking into a SQL where
     * clause. If the keys are just one name e.g. id, you'll get
     * 
     * <pre>"stage.id = base.id"</pre>
     * 
     * where "base" and "stage" are prefixes for the table names. If there are
     * two names e.g., id and cust_id, you'll get
     * 
     * <pre>"stage.id = base.id AND stage.cust_id = base.cust_id"</pre>
     * 
     * @param stagePrefix Table alias to prefix on stage table names
     * @param basePrefix Table alias to prefix on base table names
     * @return Where clause fragment or empty string if there are no primary
     *         keys
     */
    public String getPKColumnJoinList(String stagePrefix, String basePrefix)
    {
        StringBuffer joinBuf = new StringBuffer();
        List<String> pkeys = this.getPKColumns();
        for (int i = 0; i < pkeys.size(); i++)
        {
            String pkey = pkeys.get(i);
            if (i > 0)
                joinBuf.append(" AND ");
            joinBuf.append(stagePrefix);
            joinBuf.append(".");
            joinBuf.append(pkey);
            joinBuf.append(" = ");
            joinBuf.append(basePrefix);
            joinBuf.append(".");
            joinBuf.append(pkey);
        }
        return joinBuf.toString();
    }
}
