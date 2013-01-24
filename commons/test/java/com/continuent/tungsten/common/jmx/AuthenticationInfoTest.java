/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2009 Continuent Inc.
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
 * Initial developer(s): Ludovic Launer
 */

package com.continuent.tungsten.common.jmx;

import junit.framework.TestCase;

import com.continuent.tungsten.common.jmx.AuthenticationInfo.AUTH_USAGE;

/**
 * Implements a simple unit test for AuthenticationInfo
 * 
 * @author <a href="mailto:ludovic.launer@continuent.com">Ludovic Launer</a>
 * @version 1.0
 */
public class AuthenticationInfoTest extends TestCase
{
    /**
     * Tests AuthenticationNeeded and EncryptionNeeded automatic set
     */
    public void testIsAuthenticationNeeded() throws Exception
    {
        // --- Client Side ---
        AuthenticationInfo authInfo = new AuthenticationInfo(
                AUTH_USAGE.CLIENT_SIDE);

        // No username or password : not needed
        assertEquals("Authentication not needed",
                authInfo.isAuthenticationNeeded(), false);

        // Username -> needed
        authInfo.setUsername("now there's a username");
        assertEquals("Authentication needed",
                authInfo.isAuthenticationNeeded(), true);

        authInfo = new AuthenticationInfo(AUTH_USAGE.CLIENT_SIDE);
        // Password -> needed
        authInfo.setPassword("now there's a password");
        assertEquals("Authentication needed",
                authInfo.isAuthenticationNeeded(), true);

        // --- Server Side ---
        authInfo = new AuthenticationInfo(AUTH_USAGE.SERVER_SIDE);

        // No username or password : not needed
        assertEquals(authInfo.isAuthenticationNeeded(), false);

        // Username
        authInfo.setUsername("now there's a username");
        assertEquals(authInfo.isAuthenticationNeeded(), false);

        // Password
        authInfo.setPassword("now there's a password");
        assertEquals(authInfo.isAuthenticationNeeded(), false);

        // Set
        authInfo.setAuthenticationNeeded(true);
        assertEquals(authInfo.isAuthenticationNeeded(), true);

        authInfo.setAuthenticationNeeded(false);
        assertEquals(authInfo.isAuthenticationNeeded(), false);

    }

    public void testIsEncryptionNeeded() throws Exception
    {
        // --- Client Side ---
        AuthenticationInfo authInfo = new AuthenticationInfo(
                AUTH_USAGE.CLIENT_SIDE);

        // No username or password : not needed
        assertEquals("Encryption not needed", authInfo.isEncryptionNeeded(),
                false);

        // truststoreLocation -> needed
        authInfo.setTruststoreLocation("/tmp/myTruststore.ts");
        assertEquals("Encryption needed", authInfo.isEncryptionNeeded(), true);

        authInfo = new AuthenticationInfo(AUTH_USAGE.CLIENT_SIDE);
        // trustorePassword -> needed
        authInfo.setTruststorePassword("password");
        assertEquals("Encryption needed", authInfo.isEncryptionNeeded(), true);

        // --- Server Side ---
        authInfo = new AuthenticationInfo(AUTH_USAGE.SERVER_SIDE);

        // No username or password
        assertEquals(authInfo.isEncryptionNeeded(), false);

        // truststoreLocation
        authInfo.setTruststoreLocation("/tmp/myTruststore.ts");
        assertEquals(authInfo.isEncryptionNeeded(), false);

        // trustorePassword
        authInfo.setTruststorePassword("password");
        assertEquals(authInfo.isEncryptionNeeded(), false);

        // Set
        authInfo.setEncryptionNeeded(true);
        assertEquals(authInfo.isEncryptionNeeded(), true);

        authInfo.setEncryptionNeeded(false);
        assertEquals(authInfo.isEncryptionNeeded(), false);
    }

    public void testCheckAuthenticationInfo() throws Exception
    {
        AuthenticationInfo authInfo = new AuthenticationInfo(
                AUTH_USAGE.CLIENT_SIDE);
        boolean sreThrown = false;

        // If encryption required: trustore location exist
        try
        {
            authInfo.setTruststoreLocation("");
            authInfo.checkAuthenticationInfo();
        }
        catch (ServerRuntimeException sre)
        {
            assertNotNull(sre.getCause());
            sreThrown = true;
        }

        assert (sreThrown);
    }

}
