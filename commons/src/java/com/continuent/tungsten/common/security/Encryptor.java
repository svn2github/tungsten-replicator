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
import java.security.Key;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.cert.Certificate;
import java.text.MessageFormat;
import java.util.Enumeration;

import javax.crypto.Cipher;
import javax.xml.bind.DatatypeConverter;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.config.cluster.ConfigurationException;
import com.continuent.tungsten.common.jmx.ServerRuntimeException;

/**
 * Utility class to cipher / uncipher critical information based on public /
 * private key encryption
 * 
 * @author <a href="mailto:ludovic.launer@continuent.com">Ludovic Launer</a>
 * @version 1.0
 */

public class Encryptor
{
    private static final Logger logger             = Logger.getLogger(Encryptor.class);

    private AuthenticationInfo  authenticationInfo = null;

    /**
     * Creates a new <code>Encryptor</code> object
     * 
     * @param authenticationInfo
     */
    public Encryptor(AuthenticationInfo authenticationInfo)
    {
        this.authenticationInfo = authenticationInfo;

        // --- Check parameters ---
        this.authenticationInfo.checkAuthenticationInfo();
    }

    /**
     * Retrieve public and/or private keys from Keystore/Strustore Uses the
     * first Alias found in the keystore
     * 
     * @return KeyPair
     */
    public KeyPair getKeys(String storeLocation, String storePassword)
    {
        FileInputStream storeFile;
        KeyPair keyPair = null;

        try
        {
            storeFile = new FileInputStream(storeLocation);
            KeyStore keystore = KeyStore.getInstance(KeyStore.getDefaultType());

            keystore.load(storeFile, storePassword.toCharArray());

            Enumeration<String> listAliases = keystore.aliases();

            // --- Get first alias ---
            String alias = null;
            if (listAliases.hasMoreElements())
                alias = listAliases.nextElement();

            // Get certificate of public key
            Certificate cert = keystore.getCertificate(alias);
            // Get public key
            PublicKey publicKey = cert.getPublicKey();

            Key key = keystore.getKey(alias, storePassword.toCharArray());
            if (key instanceof PrivateKey)
            {
                keyPair = new KeyPair(publicKey, (PrivateKey) key);
            }
            else
            {
                keyPair = new KeyPair(publicKey, null);
            }

        }
        catch (Exception e)
        {
            String msg = MessageFormat.format(
                    "Cannot retrieve key from: {0} Reason={1}", storeLocation,
                    e.getMessage());
            logger.error(msg);
            throw new ServerRuntimeException(msg, e);
        }

        return keyPair;
    }

    /**
     * Get the Public key from a TrustStore
     * 
     * @return Public key from the Truststore
     */
    public PublicKey getPublicKey_from_Truststore()
    {
        KeyPair keyPair = this.getKeys(
                this.authenticationInfo.getTruststoreLocation(),
                this.authenticationInfo.getTruststorePassword());
        return keyPair.getPublic();
    }

    /**
     * Get the Public and Private key from a KeyStore
     * 
     * @return PrivateKey extracted from the Keystore
     */
    public PrivateKey getPrivateKey_from_KeyStore()
    {
        KeyPair keyPair = this.getKeys(
                this.authenticationInfo.getKeystoreLocation(),
                this.authenticationInfo.getKeystorePassword());
        return keyPair.getPrivate();
    }

    /**
     * Encrypt a String using public key located in truststore.
     * 
     * @param message to be encrypted
     * @return Base64 encoded and encryoted message
     */
    public String encrypt(String message)
    {
        String base64 = null;
        PublicKey publicKey = this.getPublicKey_from_Truststore();

        try
        {
            Cipher cipher = Cipher.getInstance(publicKey.getAlgorithm());
            cipher.init(Cipher.ENCRYPT_MODE, publicKey);

            // Gets the raw bytes to encrypt, UTF8 is needed for
            // having a standard character set
            byte[] stringBytes = message.getBytes("UTF8");

            // Encrypt using the cypher
            byte[] raw = cipher.doFinal(stringBytes);

            // Converts to base64 for easier display.
            base64 = DatatypeConverter.printBase64Binary(raw);
        }
        catch (Exception e)
        {
            String msg = MessageFormat.format(
                    "Cannot encrypt message. Error= {0}", e.getMessage());
            logger.error(msg);
            throw new ServerRuntimeException(msg, e);
        }

        return base64;
    }

    /**
     * Decrypt a String using private key located in KeyStore.
     * 
     * @param encryptedMessage
     * @return Decrypted String
     * @throws ConfigurationException
     */
    public String decrypt(String encryptedMessage)
            throws ConfigurationException
    {
        if (encryptedMessage == null)
            return null;

        String clearMessage = null;
        PrivateKey privateKey = this.getPrivateKey_from_KeyStore();

        try
        {
            Cipher cipher = Cipher.getInstance(privateKey.getAlgorithm());
            cipher.init(Cipher.DECRYPT_MODE, privateKey);

            // Decode the BASE64 coded message
            byte[] raw = DatatypeConverter.parseBase64Binary(encryptedMessage);

            // Decode the message
            byte[] stringBytes = cipher.doFinal(raw);

            // converts the decoded message to a String
            clearMessage = new String(stringBytes, "UTF8");      
            
        }
        catch (Exception e)
        {
            String msg = MessageFormat.format(
                    "Cannot decrypt message. Error= {0}", e.getMessage());
            throw new ConfigurationException(msg);
        }
        return clearMessage;
    }

}
