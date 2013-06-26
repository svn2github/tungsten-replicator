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
 * Contributor(s): 
 */

package com.continuent.tungsten.common.network;

/**
 * Sample ping method to test exception handling.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class SamplePingMethod implements PingMethod
{
    // Can be set to tell the sample method to throw an exception.
    public static boolean exception = false;

    private String        notes;

    /**
     * Sample test for reachability.
     * 
     * @param address Host name
     * @param timeout Timeout in milliseconds
     * @return True if host is reachable, otherwise false.
     */
    public boolean ping(HostAddress address, int timeout) throws HostException
    {
        if (exception)
        {
            notes = "exception";
            throw new HostException("exception");
        }
        else
        {
            notes = "ok";
            return true;
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.commons.network.PingMethod#getNotes()
     */
    public String getNotes()
    {
        return notes;
    }
}