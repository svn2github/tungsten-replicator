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

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.common.config.cluster.ClusterConfiguration;
import com.continuent.tungsten.common.config.cluster.ConfigurationException;
import com.continuent.tungsten.common.jmx.ServerRuntimeException;

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
     * Save passwords from a TungstenProperties into a file
     * 
     * @param authenticationInfo containing password file location
     */
    public static void saveCredentialsFromAuthenticationInfo(AuthenticationInfo authenticationInfo) throws ServerRuntimeException
    {
        String passwordFileLocation = authenticationInfo.getPasswordFileLocation();
        
        try
        {
            String username = authenticationInfo.getUsername();
            String password = authenticationInfo.getPassword();
            
            PropertiesConfiguration props = new PropertiesConfiguration(passwordFileLocation);      // Use Apache commons-configuration: preserves comments in .properties !
            props.setProperty(username, password);
            props.save();
        }
        catch (org.apache.commons.configuration.ConfigurationException ce)
        {
          logger.error("Error while saving properties for file:" + authenticationInfo.getPasswordFileLocation());
          logger.debug(ce.getMessage());
          throw new ServerRuntimeException("Error while saving Credentials: " + ce.getMessage());
        }
    }
    
    /**
     * Delete a user and password from a file
     * 
     * @param authenticationInfo containing password file location
     */
    public static void deleteUserFromAuthenticationInfo(AuthenticationInfo authenticationInfo) throws ServerRuntimeException
    {
        String username             = authenticationInfo.getUsername();
        String passwordFileLocation = authenticationInfo.getPasswordFileLocation();
        
        try
        {
            PropertiesConfiguration props = new PropertiesConfiguration(passwordFileLocation);
            
            // --- Check that the user exists ---
            String usernameInFile = props.getString(username);
            if (usernameInFile==null)
            {
                throw new ServerRuntimeException(MessageFormat.format("Username does not exist: {0}", username));
            }
            
            props.clearProperty(username);
            props.save();
        }
        catch (org.apache.commons.configuration.ConfigurationException ce)
        {
          logger.error("Error while saving properties for file:" + authenticationInfo.getPasswordFileLocation());
          logger.debug(ce.getMessage());
          throw new ServerRuntimeException("Error while saving Credentials: " + ce.getMessage());
        }
    }

    /**
     * Loads passwords from a TungstenProperties from a .properties file
     * 
     * @return TungstenProperties containing logins as key and passwords as
     *         values
     */
    public static TungstenProperties loadPasswordsFromAuthenticationInfo(
            AuthenticationInfo authenticationInfo) throws ServerRuntimeException
    {
        try
        {
            String passwordFileLocation = authenticationInfo
                    .getPasswordFileLocation();
            TungstenProperties newProps = new TungstenProperties();
            newProps.load(new FileInputStream(passwordFileLocation), false);
            newProps.trim();
            logger.debug(MessageFormat.format("Passwords loaded from: {0}",
                    passwordFileLocation));
            return newProps;
        }
        catch (FileNotFoundException e)
        {
            logger.error("Unable to find properties file: "
                    + authenticationInfo.getPasswordFileLocation());
            logger.debug("Properties search failure", e);
            throw new ServerRuntimeException("Unable to find password file: "
                    + e.getMessage());
        }
        catch (IOException e)
        {
            logger.error("Unable to read properties file: "
                    + authenticationInfo.getPasswordFileLocation());
            logger.debug("Properties read failure", e);
            throw new ServerRuntimeException("Unable to read password file: "
                    + e.getMessage());
        }
    }

    /**
     * Loads Authentication and Encryption parameters from default location for
     * service.properties file
     * 
     * @return AuthenticationInfo loaded from file
     * @throws ConfigurationException
     */
    public static AuthenticationInfo loadAuthenticationInformation()
            throws ConfigurationException
    {
        return loadAuthenticationInformation(null,
                AuthenticationInfo.AUTH_USAGE.SERVER_SIDE);
    }

    /**
     * Loads Authentication and Encryption parameters from service.properties
     * file
     * 
     * @param propertiesFileLocation Location of the security.properties file.
     *            If set to null, will try to locate default file.
     * @param authUsage
     * @return AuthenticationInfo
     * @throws ConfigurationException
     * @throws ReplicatorException
     */
    public static AuthenticationInfo loadAuthenticationInformation(
            String propertiesFileLocation,
            AuthenticationInfo.AUTH_USAGE authUsage)
            throws ConfigurationException
    {
        TungstenProperties securityProperties = loadSecurityPropertiesFromFile(propertiesFileLocation);

        AuthenticationInfo authInfo = new AuthenticationInfo(authUsage);

        // Authorisation and/or encryption
        securityProperties.trim(); // Remove white spaces
        boolean useAuthentication = securityProperties.getBoolean(
                SecurityConf.SECURITY_USE_AUTHENTICATION,
                SecurityConf.SECURITY_USE_AUTHENTICATION_DEFAULT, false);
        boolean useEncryption = securityProperties.getBoolean(
                SecurityConf.SECURITY_USE_ENCRYPTION,
                SecurityConf.SECURITY_USE_ENCRYPTION_DEFAULT, false);
        boolean useTungstenAuthenticationRealm = securityProperties
                .getBoolean(
                        SecurityConf.SECURITY_USE_TUNGSTEN_AUTHENTICATION_REALM,
                        SecurityConf.SECURITY_USE_TUNGSTEN_AUTHENTICATION_REALM_DEFAULT,
                        false);
        boolean useEncryptedPassword = securityProperties
                .getBoolean(
                        SecurityConf.SECURITY_USE_TUNGSTEN_AUTHENTICATION_REALM_ENCRYPTED_PASSWORD,
                        SecurityConf.SECURITY_USE_TUNGSTEN_AUTHENTICATION_REALM_ENCRYPTED_PASSWORD_DEFAULT,
                        false);

        // Retrieve properties
        String securityPropertiesFileLocation = securityProperties
                .getString(SecurityConf.SECURITY_PROPERTIES_PARENT_FILE_LOCATION);
        String passwordFileLocation = securityProperties
                .getString(SecurityConf.SECURITY_PASSWORD_FILE_LOCATION);
        String accessFileLocation = securityProperties
                .getString(SecurityConf.SECURITY_ACCESS_FILE_LOCATION);
        String keystoreLocation = securityProperties
                .getString(SecurityConf.SECURITY_KEYSTORE_LOCATION);
        String keystorePassword = securityProperties
                .getString(SecurityConf.SECURITY_KEYSTORE_PASSWORD);
        String truststoreLocation = securityProperties
                .getString(SecurityConf.SECURITY_TRUSTSTORE_LOCATION);
        String truststorePassword = securityProperties
                .getString(SecurityConf.SECURITY_TRUSTSTORE_PASSWORD);
        String userName = securityProperties.getString(
                SecurityConf.SECURITY_USERNAME, null, false);

        // Populate return object
        authInfo.setParentPropertiesFileLocation(securityPropertiesFileLocation);
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
        authInfo.setUsername(userName);
        
        // --- Check information is correct ---
        authInfo.checkAuthenticationInfo();                     // Checks authentication and encryption parameters: file exists, ...

        return authInfo;
    }

    /**
     * Loads Security related properties from a file. File location =
     * {clusterhome}/conf/security.properties
     * 
     * @param propertiesFileLocation location of the security.properties file.
     *            If set to null will look for the default file.
     * @return TungstenProperties containing security parameters
     * @throws ConfigurationException
     */
    private static TungstenProperties loadSecurityPropertiesFromFile(
            String propertiesFileLocation) throws ConfigurationException
    {
        TungstenProperties securityProps = null;

        // --- Get Configuration file ---
        if (propertiesFileLocation == null
                && ClusterConfiguration.getClusterHome() == null)
        {
            throw new ConfigurationException(
                    "No cluster.home found from which to configure cluster resources.");
        }

        File securityPropertiesFile;
        if (propertiesFileLocation == null) // Get from default location
        {
            ClusterConfiguration clusterConf = new ClusterConfiguration("Dummy");
            File clusterConfDirectory = clusterConf.getDir(ClusterConfiguration
                    .getGlobalConfigDirName(ClusterConfiguration
                            .getClusterHome()));
            securityPropertiesFile = new File(clusterConfDirectory.getPath(),
                    SecurityConf.SECURITY_PROPERTIES_FILE_NAME);
        }
        else
        // Get from supplied location
        {
            securityPropertiesFile = new File(propertiesFileLocation);
        }

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
        
        // Update propertiesFileLocation with the location actualy used
        securityProps.put(SecurityConf.SECURITY_PROPERTIES_PARENT_FILE_LOCATION, securityPropertiesFile.getAbsolutePath());
        
        return securityProps;
    }

}
