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
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import com.continuent.tungsten.common.config.cluster.ConfigurationException;
import com.continuent.tungsten.common.jmx.ServerRuntimeException;
import com.continuent.tungsten.common.security.AuthenticationInfo;
import com.continuent.tungsten.common.security.SecurityHelper;

/**
 * Implements utility functions for testing.
 */
public class SocketHelper
{
    /** Validate and load security properties. */
    public void loadSecurityProperties() throws ConfigurationException
    {
        AuthenticationInfo authInfo = SecurityHelper
                .loadAuthenticationInformation("sample.security.properties");
        // Validate security settings.
        if (authInfo == null)
        {
            throw new ServerRuntimeException(
                    "Unable to locate security information; ensure security.properties file is configured");
        }
    }

    /** Sends a string and confirms it is echoed back. */
    public String echo(Socket sock, String message) throws IOException
    {
        InputStream is = sock.getInputStream();
        OutputStream os = sock.getOutputStream();
        byte[] buf1 = message.getBytes();
        os.write(buf1, 0, buf1.length);
        byte[] buf2 = new byte[buf1.length];
        int offset = 0;
        int length = 0;
        while (offset < buf2.length)
        {
            length = is.read(buf2, offset, buf2.length - offset);
            offset += length;
        }
        String echoMessage = new String(buf2);
        return echoMessage;
    }
}