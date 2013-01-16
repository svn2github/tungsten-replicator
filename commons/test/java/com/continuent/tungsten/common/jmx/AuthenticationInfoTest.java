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

import org.junit.Test;

import junit.framework.TestCase;

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
        AuthenticationInfo authInfo = new AuthenticationInfo();

        // No username or password : not needed
        assertEquals("Authentication not needed",
                authInfo.isAuthenticationNeeded(), false);

        // Username -> needed
        authInfo.setUsername("now there's a username");
        assertEquals("Authentication needed",
                authInfo.isAuthenticationNeeded(), true);

        authInfo = new AuthenticationInfo();
        // Password -> needed
        authInfo.setPassword("now there's a password");
        assertEquals("Authentication needed",
                authInfo.isAuthenticationNeeded(), true);
    }

    public void testIsEncryptionNeeded() throws Exception
    {
        AuthenticationInfo authInfo = new AuthenticationInfo();

        // No username or password : not needed
        assertEquals("Encryption not needed", authInfo.isEncryptionNeeded(),
                false);

        // truststoreLocation -> needed
        authInfo.setTruststoreLocation("/tmp/myTruststore.ts");
        assertEquals("Encryption needed", authInfo.isEncryptionNeeded(), true);

        authInfo = new AuthenticationInfo();
        // trustorePassword -> needed
        authInfo.setTruststorePassword("password");
        assertEquals("Encryption needed", authInfo.isEncryptionNeeded(), true);
    }
    
    
    public void testCheckAuthenticationInfo() throws Exception
    {
        AuthenticationInfo authInfo = new AuthenticationInfo();
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
        
        assert(sreThrown);
    }
    
    

}
