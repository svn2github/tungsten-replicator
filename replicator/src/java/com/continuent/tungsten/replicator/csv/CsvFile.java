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

import com.continuent.tungsten.common.csv.CsvWriter;

/**
 * Defines information specific to a single file within a CSV file set.
 */
public class CsvFile
{
    private final CsvKey    key;
    private final File      file;
    private final CsvWriter writer;

    /**
     * Creates a new instance.
     * 
     * @param key Key for this file within CSV file set
     * @param file Location of the file on file system
     * @param writer A CSV writer for the file
     */
    public CsvFile(CsvKey key, File file, CsvWriter writer)
    {
        this.key = key;
        this.file = file;
        this.writer = writer;
    }

    public CsvKey getKey()
    {
        return key;
    }

    public File getFile()
    {
        return file;
    }

    public CsvWriter getWriter()
    {
        return writer;
    }
}
