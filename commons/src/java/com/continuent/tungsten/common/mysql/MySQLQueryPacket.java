
package com.continuent.tungsten.common.mysql;

public class MySQLQueryPacket extends MySQLPacket
{
    public MySQLQueryPacket(byte packetNumber, String query)
    {
        super(1 + query.length() + 1, packetNumber);
        putByte((byte) MySQLConstants.COM_QUERY);
        putString(query);
    }
}
