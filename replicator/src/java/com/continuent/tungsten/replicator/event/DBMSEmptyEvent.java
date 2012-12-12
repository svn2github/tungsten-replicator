/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2009-2012 Continuent Inc.
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

package com.continuent.tungsten.replicator.event;

import java.sql.Timestamp;

/**
 * This class defines a DBMSEmptyEvent
 * 
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class DBMSEmptyEvent extends DBMSEvent
{
    private static final long serialVersionUID = 1300L;

    /**
     * Creates a new empty event.
     * 
     * @param id Event Id
     * @param extractTime Time of commit or failing that extraction
     */
    public DBMSEmptyEvent(String id, Timestamp extractTime)
    {
        super(id, null, extractTime);
    }

    /**
     * Creates a new empty event with the current time as timestamp. WARNING: do
     * not put this type of event into the log as it can mess up parallel
     * replication.
     * 
     * @param id Event Id
     */
    public DBMSEmptyEvent(String id)
    {
        this(id, new Timestamp(System.currentTimeMillis()));
    }
}
