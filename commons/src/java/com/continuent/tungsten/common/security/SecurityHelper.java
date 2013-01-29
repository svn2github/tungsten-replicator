/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2013 Continuent Inc.
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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.MessageFormat;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.common.config.cluster.ClusterConfiguration;
import com.continuent.tungsten.common.config.cluster.ConfigurationException;
import com.continuent.tungsten.common.jmx.AuthenticationInfo;

/**
 * Helper class for security related topics
 * 
 * @author <a href="mailto:ludovic.launer@continuent.com">Ludovic Launer</a>
 * @version 1.0
 */
public class SecurityHelper
{
    private static final Logger logger = Logger.getLogger(SecurityHelper.class);

    /**
     * Loads Security related properties from a file. File location =
     * {clusterhome}/conf/security.properties
     * 
     * @return TungstenProperties containing security parameters
     * @throws ConfigurationException
     */
    private static TungstenProperties loadPropertiesFromFile()
            throws ConfigurationException
    {
        TungstenProperties securityProps = null;

        // --- Get Configuration file ---
        if (ClusterConfiguration.getClusterHome() == null)
        {
            throw new ConfigurationException(
                    "No cluster.home found from which to configure cluster resources.");
        }

        ClusterConfiguration clusterConf = new ClusterConfiguration("Dummy");
        File clusterConfDirectory = clusterConf.getDir(ClusterConfiguration
                .getGlobalConfigDirName(ClusterConfiguration.getClusterHome()));
        File securityPropertiesFile = new File(clusterConfDirectory.getPath(),
                SecurityConf.SECURITY_PROPERTIES_FILE_NAME);

        // --- Get properties ---
        try
        {
            securityProps = new TungstenProperties();
            securityProps.load(new FileInputStream(securityPropertiesFile),
                    false);
        }
        catch (FileNotFoundException e)
        {
            String msg = MessageFormat.format(
                    "Cannot find configuration file: {0}",
                    securityPropertiesFile.getPath());
            logger.error(msg);
            throw new ConfigurationException(msg);
        }
        catch (IOException e)
        {
            String msg = MessageFormat.format(
                    "Cannot load configuration file: {0}.\n Reason: {1}",
                    securityPropertiesFile.getPath(), e.getMessage());
            logger.error(msg);
            throw new ConfigurationException(msg);
        }

        logger.info(MessageFormat.format(
                "Security parameters loaded from: {0}",
                securityPropertiesFile.getPath()));
        return securityProps;
    }

    /**
     * Retrieves Authentication and Encryption parameters from
     * service.properties file
     * 
     * @return AuthenticationInfo
     * @throws ConfigurationException
     * @throws ReplicatorException
     */
    public static AuthenticationInfo getAuthenticationInformation()
            throws ConfigurationException
    {

        TungstenProperties tungsteProperties = loadPropertiesFromFile();

        AuthenticationInfo authInfo = new AuthenticationInfo(
                AuthenticationInfo.AUTH_USAGE.SERVER_SIDE);

        // Make a copy of the TungstenProperties so that we can Trim
        TungstenProperties jmxProperties = new TungstenProperties(
                tungsteProperties.hashMap());

        // Authorisation and/or encryption
        jmxProperties.trim(); // Remove white spaces
        boolean useAuthentication = jmxProperties.getBoolean(
                SecurityConf.SECURITY_USE_AUTHENTICATION,
                SecurityConf.SECURITY_USE_AUTHENTICATION_DEFAULT, false);
        boolean useEncryption = jmxProperties.getBoolean(
                SecurityConf.SECURITY_USE_ENCRYPTION,
                SecurityConf.SECURITY_USE_ENCRYPTION_DEFAULT, false);
        boolean useTungstenAuthenticationRealm = jmxProperties
                .getBoolean(
                        SecurityConf.SECURITY_USE_TUNGSTEN_AUTHENTICATION_REALM,
                        SecurityConf.SECURITY_USE_TUNGSTEN_AUTHENTICATION_REALM_DEFAULT,
                        false);
        boolean useEncryptedPassword = jmxProperties
                .getBoolean(
                        SecurityConf.SECURITY_USE_TUNGSTEN_AUTHENTICATION_REALM_ENCRYPTED_PASSWORD,
                        SecurityConf.SECURITY_USE_TUNGSTEN_AUTHENTICATION_REALM_ENCRYPTED_PASSWORD_DEFAULT,
                        false);

        // Retrieve properties
        String passwordFileLocation = jmxProperties
                .getString(SecurityConf.SECURITY_PASSWORD_FILE_LOCATION);
        String accessFileLocation = jmxProperties
                .getString(SecurityConf.SECURITY_ACCESS_FILE_LOCATION);
        String keystoreLocation = jmxProperties
                .getString(SecurityConf.SECURITY_KEYSTORE_LOCATION);
        String keystorePassword = jmxProperties
                .getString(SecurityConf.SECURITY_KEYSTORE_PASSWORD);
        String truststoreLocation = jmxProperties
                .getString(SecurityConf.SECURITY_TRUSTSTORE_LOCATION);
        String truststorePassword = jmxProperties
                .getString(SecurityConf.SECURITY_TRUSTSTORE_PASSWORD);

        // Populate return object
        authInfo.setAuthenticationNeeded(useAuthentication);
        authInfo.setUseTungstenAuthenticationRealm(useTungstenAuthenticationRealm);
        authInfo.setUseEncryptedPasswords(useEncryptedPassword);
        authInfo.setEncryptionNeeded(useEncryption);
        authInfo.setPasswordFileLocation(passwordFileLocation);
        authInfo.setAccessFileLocation(accessFileLocation);
        authInfo.setKeystoreLocation(keystoreLocation);
        authInfo.setKeystorePassword(keystorePassword);
        authInfo.setTruststoreLocation(truststoreLocation);
        authInfo.setTruststorePassword(truststorePassword);

        return authInfo;
    }
}
