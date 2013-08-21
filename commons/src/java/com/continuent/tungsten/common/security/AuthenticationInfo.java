/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2011 Continuent Inc.
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
import java.text.MessageFormat;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.common.config.cluster.ClusterConfiguration;
import com.continuent.tungsten.common.config.cluster.ConfigurationException;
import com.continuent.tungsten.common.jmx.ServerRuntimeException;
import com.continuent.tungsten.common.utils.CLLogLevel;
import com.continuent.tungsten.common.utils.CLUtils;

/**
 * Information class holding Authentication and Encryption parameters Some of
 * the properties may be left null depending on how and when this is used
 * 
 * @author <a href="mailto:ludovic.launer@continuent.com">Ludovic Launer</a>
 * @version 1.0
 */
public final class AuthenticationInfo
{
    private static final Logger logger                         = Logger.getLogger(AuthenticationInfo.class);
    private final AUTH_USAGE    authUsage;
    private String              parentPropertiesFileLocation    = null;    // Location of the file from which this was built

    private boolean             authenticationNeeded           = false;
    private boolean             encryptionNeeded               = false;
    private boolean             useTungstenAuthenticationRealm = true;
    private boolean             useEncryptedPasswords          = false;

    // Authentication parameters
    private String              username                       = null;
    private String              password                       = null;
    private String              passwordFileLocation           = null;
    private String              accessFileLocation             = null;
    // Encryption parameters
    private String              keystoreLocation               = null;
    private String              keystorePassword               = null;
    private String              truststoreLocation             = null;
    private String              truststorePassword             = null;

    public final static String  AUTHENTICATION_INFO_PROPERTY   = "authenticationInfo";
    public final static String  TUNGSTEN_AUTHENTICATION_REALM  = "tungstenAutenthicationRealm";
    // Possible command line parameters
    public final static String  USERNAME                       = "-username";
    public final static String  PASSWORD                       = "-password";
    public final static String  KEYSTORE_LOCATION              = "-keystoreLocation";
    public final static String  KEYSTORE_PASSWORD              = "-keystorePassword";
    public final static String  TRUSTSTORE_LOCATION            = "-truststoreLocation";
    public final static String  TRUSTSTORE_PASSWORD            = "-truststorePassword";
    public final static String  SECURITY_CONFIG_FILE_LOCATION  = "-securityProperties";

    // Defines Authentication Information flavor :
    // Server side :
    // Client side : Some of the parameters are set automatically
    public static enum AUTH_USAGE
    {
        SERVER_SIDE, CLIENT_SIDE
    };

    /**
     * Creates a new <code>AuthenticationInfo</code> object
     */
    public AuthenticationInfo(AUTH_USAGE authUsage, String parentPropertiesFileLocation)
    {
        this.authUsage = authUsage;
        this.parentPropertiesFileLocation = parentPropertiesFileLocation;
    }
    
    public AuthenticationInfo(AUTH_USAGE authUsage)
    {
        this(authUsage, null);
    }

    /**
     * Check Authentication information consistency
     * @throws  ConfigurationException 
     */
    public void checkAuthenticationInfo() throws ServerRuntimeException, ConfigurationException
    {   
        // --- Check security.properties location ---
        if (this.parentPropertiesFileLocation != null)
        {
            File f = new File(this.parentPropertiesFileLocation);
            // --- Find absolute path if needed
            if (!f.isFile())
            {
                f = this.findAbsolutePath(f);
                this.parentPropertiesFileLocation = f.getAbsolutePath();
            }
            // --- Check file is readable
            if (!f.isFile() || !f.canRead())
            {
                String msg = MessageFormat.format(
                        "Cannot find or read {0} file: {1}", SECURITY_CONFIG_FILE_LOCATION,
                        this.parentPropertiesFileLocation);
                CLUtils.println(msg, CLLogLevel.detailed);
                throw new ServerRuntimeException(msg, new AssertionError(
                        "File must exist"));
            }
        }
        // --- Check Keystore location ---
        if (this.isEncryptionNeeded() && this.keystoreLocation != null)
        {
            File f = new File(this.keystoreLocation);
            // --- Find absolute path if needed
            if (!f.isFile())
            {
                f = this.findAbsolutePath(f);
                this.keystoreLocation = f.getAbsolutePath();
            }
            // --- Check file is readable
            if (!f.isFile() || !f.canRead())
            {
                String msg = MessageFormat.format(
                        "Cannot find or read {0} file: {1}", KEYSTORE_LOCATION,
                        this.keystoreLocation);
                CLUtils.println(msg, CLLogLevel.detailed);
                throw new ServerRuntimeException(msg, new AssertionError(
                        "File must exist"));
            }
        }
        
        // --- Check Truststore location ---
        if (this.isEncryptionNeeded() && this.truststoreLocation != null)
        {
            File f = new File(this.truststoreLocation);
            // --- Find absolute path if needed
            if (!f.isFile())
            {
                f = this.findAbsolutePath(f);
                this.truststoreLocation = f.getAbsolutePath();
            }
            // --- Check file is readable
            if (!f.isFile() || !f.canRead())
            {
                String msg = MessageFormat.format(
                        "Cannot find or read {0} file: {1}",
                        TRUSTSTORE_LOCATION, this.truststoreLocation);
                CLUtils.println(msg, CLLogLevel.detailed);
                throw new ServerRuntimeException(msg, new AssertionError(
                        "File must exist"));
            }
        }
        else if (this.isEncryptionNeeded() && this.truststoreLocation == null)
        {
            throw new ConfigurationException("truststore.location");
        }
        
        // --- Check password for Truststore ---
        if (this.isEncryptionNeeded() && this.truststorePassword == null)
        {
            throw new ConfigurationException("truststore.password");
        }
        
        // --- Check password file location ---
        if (this.isAuthenticationNeeded() && this.passwordFileLocation != null)
        {
            File f = new File(this.passwordFileLocation);
            // --- Find absolute path if needed
            if (!f.isFile())
            {
                f = this.findAbsolutePath(f);
                this.passwordFileLocation = f.getAbsolutePath();
            }
            // --- Check file is readable
            if (!f.isFile() || !f.canRead())
            {
                String msg = MessageFormat.format(
                        "Cannot find or read {0} file: {1}",
                        SecurityConf.SECURITY_PASSWORD_FILE_LOCATION, this.passwordFileLocation);
                CLUtils.println(msg, CLLogLevel.detailed);
                throw new ServerRuntimeException(msg, new AssertionError(
                        "File must exist"));
            }
        }
        
     // --- Check access file location ---
        if (this.isAuthenticationNeeded() && this.accessFileLocation != null)
        {
            File f = new File(this.accessFileLocation);
            // --- Find absolute path if needed
            if (!f.isFile())
            {
                f = this.findAbsolutePath(f);
                this.accessFileLocation = f.getAbsolutePath();
            }
            // --- Check file is readable
            if (!f.isFile() || !f.canRead())
            {
                String msg = MessageFormat.format(
                        "Cannot find or read {0} file: {1}",
                        SecurityConf.SECURITY_ACCESS_FILE_LOCATION, this.accessFileLocation);
                CLUtils.println(msg, CLLogLevel.detailed);
                throw new ServerRuntimeException(msg, new AssertionError(
                        "File must exist"));
            }
        }
    }

    /**
     * Get the AuthenticationInfo as a TungstenProperties
     * 
     * @return TungstenProperties
     */
    public TungstenProperties getAsTungstenProperties()
    {
        TungstenProperties jmxProperties = new TungstenProperties();
        jmxProperties.put(AUTHENTICATION_INFO_PROPERTY, this);

        return jmxProperties;
    }

//    /**
//     * Retrieve (encrypted) password from file
//     * 
//     * @throws ConfigurationException
//     */
//    public void retrievePasswordFromFile() throws ConfigurationException
//    {
//        TungstenProperties passwordProps = SecurityHelper
//                .loadPasswordsFromAuthenticationInfo(this);
//        String username = this.getUsername();
//        String goodPassword = passwordProps.get(username);
//        this.password = goodPassword;
//
//        if (goodPassword == null)
//            throw new ConfigurationException(
//                    MessageFormat
//                            .format("Cannot find password for username= {0} \n PasswordFile={1}",
//                                    username, this.getPasswordFileLocation()));
//    }

    /**
     * Returns the decrypted password
     * 
     * @return String containing the (if needed) decrypted password
     * @throws ConfigurationException
     */
    public String getDecryptedPassword() throws ConfigurationException
    {
        String clearTextPassword = this.password;
        // --- Try to decrypt the password ---
        if (this.useEncryptedPasswords)
        {
            Encryptor encryptor = new Encryptor(this);
            clearTextPassword = encryptor.decrypt(this.password);
        }
        return clearTextPassword;
    }

    /**
     * TODO: getEncryptedPassword definition.
     * 
     * @return the encrypted password if useEncryptedPasswords==true or the
     *         clear text password otherwise
     */
    public String getPassword()
    {
        return this.password;
    }

    public void setKeystore(String keyStoreLocation, String keystorePassword)
    {
        this.setKeystoreLocation(keyStoreLocation);
        this.setKeystorePassword(keystorePassword);
    }

    public void setTruststore(String truststoreLocation,
            String truststorePassword)
    {
        this.setTruststoreLocation(truststoreLocation);
        this.setTruststorePassword(truststorePassword);
    }

    public boolean isAuthenticationNeeded()
    {
        return authenticationNeeded;
    }

    public void setAuthenticationNeeded(boolean authenticationNeeded)
    {
        this.authenticationNeeded = authenticationNeeded;
    }

    public boolean isEncryptionNeeded()
    {
        return encryptionNeeded;
    }

    public void setEncryptionNeeded(boolean encryptionNeeded)
    {
        this.encryptionNeeded = encryptionNeeded;
    }
    
    public String getKeystoreLocation()
    {
        
        return keystoreLocation;
    }

    public void setKeystoreLocation(String keystoreLocation)
    {
        this.keystoreLocation = keystoreLocation;
    }

    public String getUsername()
    {
        return username;
    }

    public void setUsername(String username)
    {
        this.username = username;
    }

    public void setPassword(String password)
    {
        this.password = password;
    }

    public String getPasswordFileLocation()
    {
        return passwordFileLocation;
    }

    public void setPasswordFileLocation(String passwordFileLocation)
    {
        this.passwordFileLocation = passwordFileLocation;
    }

    public String getAccessFileLocation()
    {
        return accessFileLocation;
    }

    public void setAccessFileLocation(String accessFileLocation)
    {
        this.accessFileLocation = accessFileLocation;
    }

    public String getKeystorePassword()
    {
        return keystorePassword;
    }

    public void setKeystorePassword(String keystorePassword)
    {
        this.keystorePassword = keystorePassword;
    }

    public String getTruststoreLocation()
    {
        return truststoreLocation;
    }

    public void setTruststoreLocation(String truststoreLocation)
    {
        this.truststoreLocation = truststoreLocation;
    }

    public String getTruststorePassword()
    {
        return truststorePassword;
    }

    public void setTruststorePassword(String truststorePassword)
    {
        this.truststorePassword = truststorePassword;
    }

    public boolean isUseTungstenAuthenticationRealm()
    {
        return useTungstenAuthenticationRealm;
    }

    public void setUseTungstenAuthenticationRealm(
            boolean useTungstenAuthenticationRealm)
    {
        this.useTungstenAuthenticationRealm = useTungstenAuthenticationRealm;
    }

    public boolean isUseEncryptedPasswords()
    {
        return useEncryptedPasswords;
    }

    public void setUseEncryptedPasswords(boolean useEncryptedPasswords)
    {
        this.useEncryptedPasswords = useEncryptedPasswords;
    }

    public String getParentPropertiesFileLocation()
    {
        return parentPropertiesFileLocation;
    }

    public void setParentPropertiesFileLocation(String parentPropertiesFileLocation)
    {
        this.parentPropertiesFileLocation = parentPropertiesFileLocation;
    }
    
    
    /**
     * Try to find a file absolute path from a series of default location
     * 
     * @param fileToFind the file for which to look for an absolute path
     * @return the file with absolute path if found. returns the same unchanged object otherwise
     */
    private File findAbsolutePath(File fileToFind)
    {
        File foundFile = fileToFind;
        
        try
        {
            String clusterHome = ClusterConfiguration.getClusterHome();
            
            if (fileToFind.getPath() == fileToFind.getName())                   // No absolute or relative path was given
            {
                // --- Try to find find in: cluster-home/conf
                File candidateFile = new File(clusterHome + File.separator + "conf" + File.separator + fileToFind.getName());
                if (candidateFile.isFile())
                {
                    foundFile = candidateFile;
                    logger.debug(MessageFormat.format("File was specified with name only, and found in default location: {0}",foundFile.getAbsoluteFile()));
                }
                else
                    throw new ConfigurationException(MessageFormat.format("File does not exist: {0}", candidateFile.getAbsolutePath()));
            }
        }
        catch (ConfigurationException e)
        {
            logger.debug(MessageFormat.format("Cannot find absolute path for file: {0} \n{1}", fileToFind.getName(), e.getMessage()));
            return fileToFind;
        }
        
        return foundFile;
    }

}
