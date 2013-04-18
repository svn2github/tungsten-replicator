package com.continuent.tungsten.replicator.extractor.oracle;
/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2011 Continuent Inc.
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


import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * 
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class OracleCDCPublication
{
    private long pubId;
    private String name;
    
    private List<String> columns;
    
    
    public OracleCDCPublication(String name, long pubId)
    {
        super();
        this.name = name;
        this.pubId = pubId;
        this.columns = new LinkedList<String>();
    }

    public void addColumn(String columnName)
    {
        columns.add(columnName);
    }

    public String getPublicationName()
    {
        return name;
    }

    public long getPublicationId()
    {
        return pubId;
    }

    public String getColumnList()
    {
        StringBuffer colList = new StringBuffer();
        for (Iterator<String> iterator = columns.iterator(); iterator.hasNext();)
        {
            String col = iterator.next();
            if(colList.length() > 0)
                colList.append(',');
            colList.append(col);
        }
        return colList.toString();
    }

    public int getColumnsCount()
    {
        return columns.size();
    }
}
