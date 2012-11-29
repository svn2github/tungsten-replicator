/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2011-2012 Continuent Inc.
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

/**
 * Represents a single batch command including the source file and starting line
 * number.
 */
public class BatchCommand
{
    private File   file;
    private int    lineNumber;
    private String rawCommand;
    private String command;

    public BatchCommand()
    {
    }

    public void setRawCommand(String rawCommand)
    {
        this.rawCommand = rawCommand;
    }

    public String getRawCommand()
    {
        return rawCommand;
    }

    public void setFile(File file)
    {
        this.file = file;
    }

    public File getFile()
    {
        return file;
    }

    public void setLineNumber(int lineNumber)
    {
        this.lineNumber = lineNumber;
    }

    public int getLineNumber()
    {
        return lineNumber;
    }

    public void setCommand(String command)
    {
        this.command = command;
    }

    public String getCommand()
    {
        return command;
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#clone()
     */
    public BatchCommand clone()
    {
        BatchCommand bc = new BatchCommand();
        bc.setFile(file);
        bc.setLineNumber(lineNumber);
        bc.setRawCommand(rawCommand);
        bc.setCommand(command);
        return bc;
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#toString()
     */
    public String toString()
    {
        StringBuffer sb = new StringBuffer();
        sb.append("command=").append(command);
        sb.append(" line=").append(lineNumber);
        if (file != null)
            sb.append(" file=").append(file.getName());
        return sb.toString();
    }
}