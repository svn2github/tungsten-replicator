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

import java.util.Collections;

import javax.management.remote.JMXAuthenticator;
import javax.management.remote.JMXPrincipal;
import javax.security.auth.Subject;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.common.config.cluster.ConfigurationException;
import com.continuent.tungsten.common.jmx.JmxManager;
import com.continuent.tungsten.common.security.PasswordManager.ClientApplicationType;

/**
 * Custom Authentication Realm
 * 
 * @author <a href="mailto:ludovic.launer@continuent.com">Ludovic Launer</a>
 * @version 1.0
 */
public class RealmJMXAuthenticator implements JMXAuthenticator
{
    private static final Logger logger                 = Logger.getLogger(JmxManager.class);
//    private TungstenProperties  passwordProps          = null;

    private AuthenticationInfo  authenticationInfo     = null;
    private PasswordManager     passwordManager        = null;

    private static final String INVALID_CREDENTIALS    = "Invalid credentials";
    private static final String AUTHENTICATION_PROBLEM = "Error while trying to authenticate";

    public RealmJMXAuthenticator(AuthenticationInfo authenticationInfo) throws ConfigurationException
    {
        this.authenticationInfo = authenticationInfo;
        this.passwordManager    = new PasswordManager(authenticationInfo.getParentPropertiesFileLocation(), ClientApplicationType.RMI_JMX);

//        this.passwordProps = SecurityHelper
//                .loadPasswordsFromAuthenticationInfo(authenticationInfo);
    }

    /**
     * Authenticate {@inheritDoc}
     * 
     * @see javax.management.remote.JMXAuthenticator#authenticate(java.lang.Object)
     */
    public Subject authenticate(Object credentials)
    {
        boolean authenticationOK = false;

        String[] aCredentials = this.checkCredentials(credentials);

        // --- Get auth parameters ---
        String username = (String) aCredentials[0];
        String password = (String) aCredentials[1];
        String realm = (String) aCredentials[2];

        // --- Perform authentication ---
        try
        {
            // Password file syntax:
            // username=password
//            String goodPassword = this.passwordProps.get(username);
            String goodPassword = this.passwordManager.getClearTextPasswordForUser(username);
//            this.authenticationInfo.setPassword(goodPassword);
//            // Decrypt password if needed
//            goodPassword = this.authenticationInfo.getPassword();

            if (goodPassword.equals(password))
                authenticationOK = true;

        }
        catch (Exception e)
        {
            // Throw same exception as authentication not OK :
            // Do not give any hint on failure reason
            throw new SecurityException(AUTHENTICATION_PROBLEM);
        }

        if (authenticationOK)
        {
            return new Subject(true, Collections.singleton(new JMXPrincipal(
                    username)), Collections.EMPTY_SET, Collections.EMPTY_SET);
        }
        else
        {
            throw new SecurityException(INVALID_CREDENTIALS);
        }
    }

    /**
     * Check credentials are OK
     * 
     * @param credentials
     * @return String[] containing {username, password, realm}
     */
    private String[] checkCredentials(Object credentials)
    {
        // Verify that credentials is of type String[].
        if (!(credentials instanceof String[]))
        {
            // Special case for null so we get a more informative message
            if (credentials == null)
            {
                throw new SecurityException("Credentials required");
            }
            throw new SecurityException("Credentials should be String[]");
        }

        // Verify that the array contains three elements
        // (username/password/realm).
        final String[] aCredentials = (String[]) credentials;
        if (aCredentials.length != 3)
        {
            throw new SecurityException("Credentials should have 3 elements");
        }

        return aCredentials;
    }

}
