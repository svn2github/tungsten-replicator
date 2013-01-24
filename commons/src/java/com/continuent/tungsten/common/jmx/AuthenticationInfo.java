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
    public AuthenticationInfo(AUTH_USAGE authUsage)
    {
        this.authUsage = authUsage;
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

        // Check Keystore location
        if (this.isEncryptionNeeded() && this.keystoreLocation != null)
        {
            File f = new File(this.keystoreLocation);
            if (!f.isFile() || !f.canRead())
            {
                String msg = MessageFormat.format(
                        "Cannot find or read {0} file: {1}", KEYSTORE_LOCATION,
                        this.keystoreLocation);
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
        if (this.authUsage == AUTH_USAGE.CLIENT_SIDE
                && (this.getUsername() != null || this.getPassword() != null))
            this.authenticationNeeded = true;

        return authenticationNeeded;
    }

    public void setAuthenticationNeeded(boolean authenticationNeeded)
    {
        this.authenticationNeeded = authenticationNeeded;
    }

    public boolean isEncryptionNeeded()
    {
        if (this.authUsage == AUTH_USAGE.CLIENT_SIDE
                && (this.getTruststoreLocation() != null || this
                        .getTruststorePassword() != null))
            this.encryptionNeeded = true;

        return encryptionNeeded;
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

}
