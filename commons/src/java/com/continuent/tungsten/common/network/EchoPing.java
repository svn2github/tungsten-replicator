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
 * Initial developer(s): Csaba Endre Simon
 * Contributor(s): 
 */

package com.continuent.tungsten.common.network;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.common.network.Echo.EchoStatus;

/**
 * Tests for reachability using the echo server.
 * 
 * @author <a href="mailto:csimon@vmware.com">Csaba Endre Simon</a>
 */
public class EchoPing implements PingMethod
{
    private final static int DEFAULT_ECHO_PORT = 7;

    private String           notes             = "EchoPing";

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.common.network.PingMethod#ping()
     */
    @Override
    public boolean ping(HostAddress address, int timeout)
    {
        TungstenProperties response = Echo.isReachable(address.getHostName(),
                DEFAULT_ECHO_PORT, timeout);
        return response.getObject(Echo.STATUS_KEY) == EchoStatus.OK;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.common.network.PingMethod#getNotes()
     */
    @Override
    public String getNotes()
    {
        return notes;
    }
}