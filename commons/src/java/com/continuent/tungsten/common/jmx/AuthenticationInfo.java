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

package com.continuent.tungsten.common.jmx;

import java.io.File;
import java.text.MessageFormat;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.config.TungstenProperties;

/**
 * Information class holding Authentication and Encryption parameters
 * 
 * @author <a href="mailto:ludovic.launer@continuent.com">Ludovic Launer</a>
 * @version 1.0
 */
public final class AuthenticationInfo
{
    private static final Logger logger                       = Logger.getLogger(AuthenticationInfo.class);

    private boolean             authenticationNeeded         = false;
    private boolean             encryptionNeeded             = false;

    // Authentication parameters
    private String              username                     = null;
    private String              password                     = null;
    private String              passwordFileLocation         = null;
    private String              accessFileLocation           = null;
    // Encryption parameters
    private String              keystoreLocation             = null;
    private String              keystorePassword             = null;
    private String              truststoreLocation           = null;
    private String              truststorePassword           = null;

    public final static String  AUTHENTICATION_INFO_PROPERTY = "authenticationInfo";
    // Possible command line parameters
    public final static String  USERNAME                     = "-username";
    public final static String  PASSWORD                     = "-password";
    public final static String  KEYSTORE_LOCATION            = "-keystoreLocation";
    public final static String  KEYSTORE_PASSWORD            = "-keystorePassword";
    public final static String  TRUSTSTORE_LOCATION          = "-truststoreLocation";
    public final static String  TRUSTSTORE_PASSWORD          = "-truststorePassword";

    /**
     * Creates a new <code>AuthenticationInfo</code> object
     */
    public AuthenticationInfo()
    {

    }

    /**
     * Creates a new <code>AuthenticationInfo</code> object
     * 
     * @param _useAuthentication
     * @param _useEncryption
     * @param _username
     * @param _password
     * @param _passwordFileLocation
     * @param _accessFileLocation
     * @param _keystoreLocation
     * @param _keystorePassword
     * @param _truststoreLocation
     * @param _truststorePassword
     */
    public AuthenticationInfo(boolean _useAuthentication,
            boolean _useEncryption, String _username, String _password,
            String _passwordFileLocation, String _accessFileLocation,
            String _keystoreLocation, String _keystorePassword,
            String _truststoreLocation, String _truststorePassword)
    {
        // Authentication and encryption parameters
        authenticationNeeded = _useAuthentication;
        encryptionNeeded = _useEncryption;

        this.checkAuthenticationInfo();

        username = _username;
        password = _password;
        passwordFileLocation = _passwordFileLocation;
        accessFileLocation = _accessFileLocation;
        keystoreLocation = _keystoreLocation;
        keystorePassword = _keystorePassword;
        truststoreLocation = _truststoreLocation;
        truststorePassword = _truststorePassword;
    }

    /**
     * Check Authentication information consistency
     * 
     * @return
     */
    public void checkAuthenticationInfo() throws ServerRuntimeException
    {
        // Check Truststore location
        if (this.isEncryptionNeeded() && this.truststoreLocation != null)
        {
            File f = new File(this.truststoreLocation);
            if (!f.isFile() || !f.canRead())
            {
                String msg = MessageFormat.format(
                        "Cannot find or read {0} file: {1}",
                        TRUSTSTORE_LOCATION, this.truststoreLocation);
                logger.error(msg);
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

    public boolean isAuthenticationNeeded()
    {
        if (this.username != null || this.password != null)
            authenticationNeeded = true;

        return authenticationNeeded;
    }

    public boolean isEncryptionNeeded()
    {
        if (this.truststoreLocation != null || this.truststorePassword != null)
            encryptionNeeded = true;

        return encryptionNeeded;
    }

    public void setAuthenticationNeeded(boolean authenticationNeeded)
    {
        this.authenticationNeeded = authenticationNeeded;
    }

    public void setEncryptionNeeded(boolean encryptionNeeded)
    {
        this.encryptionNeeded = encryptionNeeded;
    }

    public String getUsername()
    {
        return username;
    }

    public void setUsername(String username)
    {
        this.username = username;
    }

    public String getPassword()
    {
        return password;
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

    public String getKeystoreLocation()
    {
        return keystoreLocation;
    }

    public void setKeystoreLocation(String keystoreLocation)
    {
        this.keystoreLocation = keystoreLocation;
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

}
