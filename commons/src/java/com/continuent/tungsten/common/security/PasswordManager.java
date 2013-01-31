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
 * Contributor(s): 
 */

package com.continuent.tungsten.common.security;

import java.text.MessageFormat;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.common.config.cluster.ConfigurationException;
import com.continuent.tungsten.common.jmx.ServerRuntimeException;
import com.continuent.tungsten.common.security.AuthenticationInfo.AUTH_USAGE;

/**
 * Class managing passwords in a file. Creates, deletes, updates
 * 
 * @author <a href="mailto:ludovic.launer@continuent.com">Ludovic Launer</a>
 * @version 1.0
 */
public class PasswordManager
{
    private static Logger      logger                         = Logger.getLogger(PasswordManager.class);

    // Authentication and Encryption information
    private AuthenticationInfo authenticationInfo             = null;
    private TungstenProperties passwordsProperties            = null;

    /**
     * Creates a new <code>PasswordManager</code> object Loads Security related
     * properties from a file. File location =
     * {clusterhome}/conf/security.properties
     * 
     * @param propertiesFileLocation location of the security.properties file.
     *            If set to null will look for the default file.
     * @throws ConfigurationException
     */
    public PasswordManager(String securityPropertiesFileLocation)
            throws ConfigurationException
    {
        // --- Try to get Security information from properties file ---
        // If securityPropertiesFileLocation==null will try to locate
        // default file
        try
        {
            this.authenticationInfo = SecurityHelper
                    .loadAuthenticationInformation(
                            securityPropertiesFileLocation,
                            AUTH_USAGE.CLIENT_SIDE);
        }
        catch (ConfigurationException ce)
        {
            logger.debug(MessageFormat.format("Configuration error: {0}",
                    ce.getMessage()));
            throw ce;
        }
        catch (ServerRuntimeException sre)
        {
            logger.debug(MessageFormat.format(
                    "Could not get authentication information : {0}",
                    sre.getMessage()));
        }
    }

    /**
     * Passwords loaded from file as TungstenProperties. Example:
     * getPasswordsAsTungstenProperties.get(username);
     * 
     * @return
     */
    public TungstenProperties loadPasswordsAsTungstenProperties()
            throws ServerRuntimeException
    {
        this.passwordsProperties = SecurityHelper
                .loadPasswordsFromAuthenticationInfo(this.authenticationInfo);
        return passwordsProperties;
    }

    /**
     * Get clear text password for a username.
     * The list of passwords is loaded from the file if it hasn't been done before
     * @param username
     * @return
     * @throws ConfigurationException
     */
    public String getPasswordForUser(String username) throws ConfigurationException
    {
        String clearTextPassword = null;
        // --- Load passwords from file if necessary
        if (this.passwordsProperties==null)
            this.loadPasswordsAsTungstenProperties();
   
        this.authenticationInfo.setUsername(username);
        this.authenticationInfo.setPassword(this.passwordsProperties.get(username));
        
        clearTextPassword = this.authenticationInfo.getPassword();
        
        return clearTextPassword;
    }

    public AuthenticationInfo getAuthenticationInfo()
    {
        return authenticationInfo;
    }

    public void setAuthenticationInfo(AuthenticationInfo authenticationInfo)
    {
        this.authenticationInfo = authenticationInfo;
        this.passwordsProperties = null;
    }

}
