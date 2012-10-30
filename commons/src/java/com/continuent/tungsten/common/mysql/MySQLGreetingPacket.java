
package com.continuent.tungsten.common.mysql;

public abstract class MySQLGreetingPacket
{
    /**
     * Extracts the seed encoded in this packet.<br>
     * The seed is the concatenation of two arrays of bytes found in this
     * packet.
     * 
     * @return the server seed to salt password with for password encoding
     *         purposes
     */
    public static byte[] getSeed(MySQLPacket p)
    {
        // discard what we don't need, get the seed
        p.getByte();
        p.getString();
        p.getInt32();
        // here is the first part of the seed
        byte[] seed1 = p.getBytes(8);
        p.getByte();
        p.getShort();
        p.getByte();
        p.getShort();
        p.getBytes(13);
        // here is the second part
        byte[] seed2 = p.getBytes(12);
        // construct the final seed
        byte[] finalSeed = new byte[seed1.length + seed2.length];
        System.arraycopy(seed1, 0, finalSeed, 0, seed1.length);
        System.arraycopy(seed2, 0, finalSeed, seed1.length, seed2.length);
        return finalSeed;
    }
}
