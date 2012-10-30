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

import java.net.InetAddress;

/**
 * Provides a wrapper on the standard InetAddress that supports extended methods
 * for determining host reachability.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class HostAddress
{
    private final InetAddress address;

    /**
     * Create a new host address on a given InetAddress.
     * 
     * @param address InetAddress to use
     */
    public HostAddress(InetAddress address)
    {
        this.address = address;
    }

    /**
     * Returns the InetAddress corresponding to this host name.
     */
    public InetAddress getInetAddress()
    {
        return address;
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    public boolean equals(Object arg0)
    {
        return address.equals(arg0);
    }

    /**
     * @see java.net.InetAddress#getAddress()
     */
    public byte[] getAddress()
    {
        return address.getAddress();
    }

    /**
     * @see java.net.InetAddress#getCanonicalHostName()
     */
    public String getCanonicalHostName()
    {
        return address.getCanonicalHostName();
    }

    /**
     * @see java.net.InetAddress#getCanonicalHostName()
     */
    public String getHostAddress()
    {
        return address.getHostAddress();
    }

    public String getHostName()
    {
        return address.getHostName();
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#hashCode()
     */
    public int hashCode()
    {
        return address.hashCode();
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#toString()
     */
    public String toString()
    {
        return address.toString();
    }
}