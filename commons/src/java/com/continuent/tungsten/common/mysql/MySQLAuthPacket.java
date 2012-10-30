
package com.continuent.tungsten.common.mysql;

/**
 * Defines the authentication packet, the one that goes out after a
 * {@link MySQLGreetingPacket}
 * 
 * @author <a href="mailto:gilles.rayrat@continuent.com">Gilles Rayrat</a>
 */
public class MySQLAuthPacket extends MySQLPacket
{
    /**
     * Constructs an authentication packet with the given credentials
     * 
     * @param packetNumber for packet sequence, should be greeting packet number
     *            +1
     * @param user user name
     * @param encryptedPassword the password encoded the MySQL way
     * @param db database name
     */
    MySQLAuthPacket(byte packetNumber, String user, byte[] encryptedPassword,
            String db)
    {
        super(4 /* client_flags */+ 4/* max_packet_size */
                + 1/* charset_number */+ 23/* filler */+ user.length() + 1
                + encryptedPassword.length + 1
                + (db == null || db.length() == 0 ? 0 : db.length() + 1),
                packetNumber);
        int flags = 1/* CLIENT_LONG_PASSWORD */
                | (db == null || db.length() == 0 ? 0 : 8/* CLIENT_CONNECT_WITH_DB */
                ) | 128 /* LOCAL_FILES */| 256/* CLIENT_IGNORE_SPACE */| 512/* CLIENT_PROTOCOL_41 */
                | 8192 /* TRANSACTIONS */| 32768/* CLIENT_SECURE_CONNECTION */;
        putInt32(flags);
        putInt32(0x00ffffff); /* max allowed packet */
        putByte((byte) 0x21); /* unicode */
        putBytes(new byte[23]); /* filler */
        putString(user);
        putLenBytes(encryptedPassword);
        if (db != null && db.length() > 0)
        {
            putString(db);
        }
    }
}
