/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2013 Continuent Inc.
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

package com.continuent.tungsten.common.sockets;

import java.io.IOException;

/**
 * Denotes an exception that occurs when an operation on a socket fails due to
 * the socket being closed in another other thread.
 */
public class SocketTerminationException extends IOException
{
    private static final long serialVersionUID = 1L;

    /**
     * Instantiate a new exception, which includes the underlying exception
     * trapped by the socket wrapper code.
     */
    SocketTerminationException(String msg, IOException trappedException)
    {
        super(msg, trappedException);
    }
}
