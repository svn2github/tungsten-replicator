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

/**
 * This class defines a SecurityConf This matches parameters used for
 * Authentication and Encryption
 * 
 * @author Ludovic Launer
 * @version 1.0
 */
public class SecurityConf
{
    static public final String SECURITY_PROPERTIES_PARENT_FILE_LOCATION                              = "security.properties.parent.file.location";      // Location of the file where this is all coming from
    static public final String SECURITY_PROPERTIES_FILE_NAME                                         = "security.properties";

    /** Authentication and Encryption */
    static public final String SECURITY_USE_AUTHENTICATION                                           = "security.use.authentication";
    static public final String SECURITY_USE_TUNGSTEN_AUTHENTICATION_REALM                            = "security.use.tungsten.authenticationRealm";
    static public final String SECURITY_USE_TUNGSTEN_AUTHENTICATION_REALM_ENCRYPTED_PASSWORD         = "security.use.tungsten.authenticationRealm.encrypted.password";
    static public final String SECURITY_USE_ENCRYPTION                                               = "security.use.encryption";
    static public final String SECURITY_PASSWORD_FILE_LOCATION                                       = "security.password_file.location";
    static public final String SECURITY_ACCESS_FILE_LOCATION                                         = "security.access_file.location";
    static public final String SECURITY_KEYSTORE_LOCATION                                            = "security.keystore.location";
    static public final String SECURITY_KEYSTORE_PASSWORD                                            = "security.keystore.password";
    static public final String SECURITY_TRUSTSTORE_LOCATION                                          = "security.truststore.location";
    static public final String SECURITY_TRUSTSTORE_PASSWORD                                          = "security.truststore.password";
    static public final String SECURITY_USERNAME                                                     = "security.username";

    /** Authentication and Encryption: DEFAULT values */
    static public final String SECURITY_USE_AUTHENTICATION_DEFAULT                                   = "false";
    static public final String SECURITY_USE_ENCRYPTION_DEFAULT                                       = "false";
    static public final String SECURITY_USE_TUNGSTEN_AUTHENTICATION_REALM_DEFAULT                    = "true";
    static public final String SECURITY_USE_TUNGSTEN_AUTHENTICATION_REALM_ENCRYPTED_PASSWORD_DEFAULT = "false";

    /** Application specific information */
    static public final String SECURITY_APPLICATION_RMI_JMX                                          = "rmi_jmx";
    static public final String SECURITY_APPLICATION_CONNECTOR                                        = "connector";

}
