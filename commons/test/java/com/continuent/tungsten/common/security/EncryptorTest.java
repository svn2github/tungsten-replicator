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

import java.security.KeyPair;

import junit.framework.TestCase;

import com.continuent.tungsten.common.config.cluster.ConfigurationException;
import com.continuent.tungsten.common.jmx.ServerRuntimeException;

/**
 * Implements a simple unit test for AuthenticationInfo
 * 
 * @author <a href="mailto:ludovic.launer@continuent.com">Ludovic Launer</a>
 * @version 1.0
 */
public class EncryptorTest extends TestCase
{
    AuthenticationInfo authInfo = new AuthenticationInfo();
    Encryptor encryptor = null;
    
    public EncryptorTest()
    {
        this.authInfo.setKeystore("tungsten_sample_keystore.jks", "secret");
        this.authInfo.setTruststore("tungsten_sample_truststore.ts", "secret");
        
//        authInfo.setKeystore("tungsten_keystore.jks", "tungsten");
//        authInfo.setTruststore("tungsten_truststore.ts", "tungsten");
        
        try
        {
            this.encryptor = new Encryptor(authInfo);
        }
        catch (ServerRuntimeException e)
        {
            assertTrue(false);
        }
        catch (ConfigurationException e)
        {
            assertTrue(false);
        }
    }
    
    /**
     * Tests encryption / decryption
     */
    public void testEncrytion() throws Exception
    {
        // --- Test encryption / decryption ---
        String testString       = "secret";
        String someRandomString = "and now, for something completly different";
        
        String encryptedString = encryptor.encrypt(testString);
        String decryptedString = encryptor.decrypt(encryptedString);
        
        assertEquals(testString, decryptedString);
        assertFalse(testString.equals(someRandomString));
    }
    
    public void testGetKeysOnKeystore()
    {
        // -- Keystore--
        KeyPair keyPair = encryptor.getKeys(this.authInfo.getKeystoreLocation(), this.authInfo.getKeystorePassword());
        
        assertNotNull(keyPair.getPrivate());
        assertNotNull(keyPair.getPublic());
    }
    
    public void testGetKeysOnTruststore()
    {
        // -- Keystore--
        KeyPair keyPair = encryptor.getKeys(this.authInfo.getTruststoreLocation(), this.authInfo.getTruststorePassword());
        
        assertNull(keyPair.getPrivate());
        assertNotNull(keyPair.getPublic());
    }

   
    

}
