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

import junit.framework.TestCase;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.common.config.cluster.ConfigurationException;

/**
 * Implements a simple unit test for SecurityHelper
 * 
 * @author <a href="mailto:ludovic.launer@continuent.com">Ludovic Launer</a>
 * @version 1.0
 */
public class SecurityHelperTest extends TestCase
{
    /**
     * Test we can retrieve passwords from the passwords.store file
     * 
     * @throws Exception
     */
    public void testLoadPasswordsFromFile() throws Exception
    {
        AuthenticationInfo authenticationInfo = new AuthenticationInfo();
        authenticationInfo.setPasswordFileLocation("sample.passwords.store");

        // This should not be null if we retrieve passwords from the file
        TungstenProperties tungsteProperties = SecurityHelper
                .loadPasswordsFromAuthenticationInfo(authenticationInfo);
        assertNotNull(tungsteProperties);
    }

    /**
     * Test we can retrieve authentication information from the
     * security.properties file
     * 
     * @throws ConfigurationException
     */
    public void testloadAuthenticationInformation()
            throws ConfigurationException
    {
        // Get authInfo from the configuration file on the CLIENT_SIDE
        AuthenticationInfo authInfo = SecurityHelper
                .loadAuthenticationInformation("sample.security.properties");
        assertNotNull(authInfo);

        // Get authInfo from the configuration file on the SERVER_SIDE
        authInfo = SecurityHelper.loadAuthenticationInformation(
                "sample.security.properties");
        assertNotNull(authInfo);

        // Check that an Exception is thrown when the configuration file is not found
        ConfigurationException configurationException = null;
        try
        {
            authInfo = SecurityHelper.loadAuthenticationInformation(
                    "sample.security.properties_DOES_NOT_EXIST");
        }
        catch (ConfigurationException ce)
        {
            configurationException = ce;
        }
        assertNotNull(configurationException);

    }
    
    /**
     * Confirm that we can retrieve values from the security.properties file
     * 
     * @throws ConfigurationException
     */
    public void testRetrieveInformation() throws ConfigurationException
    {
        // Get authInfo from the configuration file on the SERVER_SIDE
        AuthenticationInfo authInfo = SecurityHelper.loadAuthenticationInformation( "sample.security.properties");
        assertNotNull(authInfo);
        
        TungstenProperties securityProp = authInfo.getAsTungstenProperties();
        assertNotNull(securityProp);
        
        Boolean useJmxAuthentication = securityProp.getBoolean(SecurityConf.SECURITY_JMX_USE_AUTHENTICATION);
        assertNotNull(useJmxAuthentication);
    }

    
    /**
     * Confirm that once we have loaded the security information, it becomes available in system properties
     * @throws ConfigurationException 
     *
     */
    public void testloadAuthenticationInformation_and_setSystemProperties() throws ConfigurationException
    {
        // Get authInfo from the configuration file on the SERVER_SIDE
        AuthenticationInfo authInfo = SecurityHelper.loadAuthenticationInformation( "sample.security.properties");
        assertNotNull(authInfo);
        
        // Check it's available in system wide properties
        String systemProperty = null;
        systemProperty = System.getProperty("javax.net.ssl.keyStore", null);
        assertNotNull(systemProperty);
        
        systemProperty = System.getProperty("javax.net.ssl.keyStorePassword", null);
        assertNotNull(systemProperty);
        
        systemProperty = System.getProperty("javax.net.ssl.trustStore", null);
        assertNotNull(systemProperty);
        
        systemProperty = System.getProperty("javax.net.ssl.trustStorePassword", null);
        assertNotNull(systemProperty);
    }

}
