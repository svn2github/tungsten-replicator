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

package com.continuent.tungsten.replicator.extractor.mysql;

/**
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public interface LoadDataInfileEvent
{
    /**
     * Returns the file ID of this Load Data Infile command.
     * 
     * @return a file ID
     */
    public int getFileID();

    /**
     * Sets whether the next event of this event can be appended to it in the
     * same THL event, i.e. if it is part of the same load data infile command.
     * 
     * @param b
     */
    public void setNextEventCanBeAppended(boolean b);

    /**
     * Indicates whether next event in the binlog can be appended to this one.
     * This is possible if the next event is part of the same Load Data Infile
     * command.
     * 
     * @return true if next event can be appended, false otherwise.
     */
    public boolean canNextEventBeAppended();

}
