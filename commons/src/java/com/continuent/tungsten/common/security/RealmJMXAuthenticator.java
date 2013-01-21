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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;

import javax.management.remote.JMXAuthenticator;
import javax.management.remote.JMXPrincipal;
import javax.security.auth.Subject;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.common.jmx.JmxManager;
import com.continuent.tungsten.common.jmx.ServerRuntimeException;

public class RealmJMXAuthenticator implements JMXAuthenticator
{
    private static final Logger logger              = Logger.getLogger(JmxManager.class);
    private TungstenProperties  passwordProps       = null;

    private final String        passwordFileLocation;

    private static final String INVALID_CREDENTIALS = "Invalid credentials";

    public RealmJMXAuthenticator(String passwordFileLocation)
    {
        this.passwordFileLocation = passwordFileLocation;
        this.passwordProps = this.LoadPasswordsFromFile();
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
            String goodPassword = this.passwordProps.get(username);
            if (goodPassword.equals(password))
                authenticationOK = true;
        }
        catch (Exception e)
        {
            // Throw same exception as authentication not OK : 
            // Do not give any hint on failure reason
            throw new SecurityException(INVALID_CREDENTIALS);
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
     * Loads passwords into a TungstenProperties from a .properties file
     * 
     * @return TungstenProperties containing logins as key and passwords as
     *         values
     */
    private TungstenProperties LoadPasswordsFromFile()
    {
        try
        {
            TungstenProperties newProps = new TungstenProperties();
            newProps.load(new FileInputStream(this.passwordFileLocation), false);
            newProps.trim();
            return newProps;
        }
        catch (FileNotFoundException e)
        {
            logger.error("Unable to find properties file: "
                    + this.passwordFileLocation);
            logger.debug("Properties search failure", e);
            throw new ServerRuntimeException("Unable to find properties file: "
                    + e.getMessage());
        }
        catch (IOException e)
        {
            logger.error("Unable to read properties file: "
                    + this.passwordFileLocation);
            logger.debug("Properties read failure", e);
            throw new ServerRuntimeException("Unable to read properties file: "
                    + e.getMessage());
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
