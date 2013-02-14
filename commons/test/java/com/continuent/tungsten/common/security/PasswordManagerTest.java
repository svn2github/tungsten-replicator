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

package com.continuent.tungsten.common.security;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.common.config.cluster.ConfigurationException;
import com.continuent.tungsten.common.jmx.ServerRuntimeException;
import com.continuent.tungsten.common.security.PasswordManager.ClientApplicationType;

import junit.framework.TestCase;

/**
 * Implements a simple unit test for AuthenticationInfo
 * 
 * @author <a href="mailto:ludovic.launer@continuent.com">Ludovic Launer</a>
 * @version 1.0
 */
public class PasswordManagerTest extends TestCase
{

    /**
     * Create an instance of a PasswordManager reading from a
     * security.properties file
     * 
     * @throws ConfigurationException
     */
    public void testCreatePasswordManager() throws ConfigurationException
    {
        // This file should load just fine
        PasswordManager pwd = new PasswordManager("sample.security.properties");
        assertTrue(true);

        // This one should raise an exception as it does not exist
        try
        {
            pwd = new PasswordManager(
                    "sample.security.properties_DOES_NOT_EXIST");
            assertTrue(false);
        }
        catch (ConfigurationException e)
        {
            assertTrue(true);
        }

    }

    /**
     * Test loading passwords from a file
     * Should succeed with correct password file location
     * Should throw an Exception if the password file location is not correct
     * 
     * @throws ConfigurationException
     */
    public void testloadPasswordsAsTungstenProperties()
            throws ConfigurationException
    {
        // --- Load passwords from existing file ---
        // This file should load just fine
        PasswordManager pwd = new PasswordManager("sample.security.properties");

        // List of passwords is popualted once we have loaded it
        TungstenProperties passwdProps = pwd.loadPasswordsAsTungstenProperties();
        assertEquals(true, passwdProps.size() != 0);
        
        // And we can retrieve a password for an existing user
        String goodPassword = passwdProps.get("tungsten");
        assertNotNull(goodPassword);
        
        // --- Load passwords from a non existing file ---
        // We modify the password file location so that it does not exist
        AuthenticationInfo authInfo = pwd.getAuthenticationInfo();
        authInfo.setPasswordFileLocation(authInfo.getPasswordFileLocation() + "_DOES_NOT_EXIST");
        
        // We should now have an exception when trying to get passwords
        try
        {
        passwdProps = pwd.loadPasswordsAsTungstenProperties();
        assertTrue(false);
        }
        catch (ServerRuntimeException  e)
        {
            assertTrue(true);
        }
        
    }
    
    /**
     * Test retrieving passwords from file.
     * Passwords are returned in clear text even if they were encoded
     * Decryption is done in the AuthenticationInfo class
     * 
     * @throws ConfigurationException
     */
    public void testgetPasswordForUser() throws ConfigurationException
    {
        PasswordManager pwd = null;
        String goodPassword = null;
        try
        {
            pwd = new PasswordManager("sample.security.properties");
        }
        catch (ConfigurationException e)
        {
            assertTrue(false);
        }
        
        // Try to get password without having loaded the passwords
        goodPassword = pwd.getClearTextPasswordForUser("tungsten");
        assertNotNull(goodPassword);
        
        // Get a password for a non existing user
        goodPassword = pwd.getClearTextPasswordForUser("non_existing_user");
        assertNull(goodPassword);
    }
    
    /**
     * Test retrieving passwords from file : Application Specific
     * Passwords are returned in clear text even if they were encoded
     * Decryption is done in the AuthenticationInfo class
     * Gets application specific user
     * 
     * @throws ConfigurationException
     */
    public void testgetPasswordForUser_by_Application() throws ConfigurationException
    {
        PasswordManager pwd             = null;
        String goodPassword             = null;
        String goodEncryptedPassword    = null;
        try
        {
            pwd = new PasswordManager("sample.security.properties", ClientApplicationType.RMI_JMX);
        }
        catch (ConfigurationException e)
        {
            assertTrue(false);
        }
        
        // Try to get password without having loaded the passwords
        goodEncryptedPassword   = pwd.getEncryptedPasswordForUser("tungstenRMI");
        goodPassword            = pwd.getClearTextPasswordForUser("tungstenRMI");
        
        assertNotNull(goodEncryptedPassword);                   // We should get something
        assertNotNull(goodPassword);
        
        assertTrue(goodEncryptedPassword != goodPassword);      // Clear text and encyprted password should be different
        
        assertEquals("secret", goodPassword);                   // The expected clear text password = secret
        

        // Get a password for a non existing user
        goodPassword = pwd.getClearTextPasswordForUser("non_existing_user");
        assertNull(goodPassword);
    }
    
    
    

}
