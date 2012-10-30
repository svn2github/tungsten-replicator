/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2009 Continuent Inc.
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
 * Initial developer(s): Robert Hodges
 * Contributor(s): 
 */

package com.continuent.tungsten.common.parsing.bytes;

import java.io.UnsupportedEncodingException;

/**
 * Implements a translation buffer for complex byte strings that allows clients
 * to translate parts of the buffer directly to Unicode and to substitute other
 * parts with different values.
 * 
 * @author <a href="mailto:jussi-pekka.kurikka@continuent.com">Jussi-Pekka
 *         Kurikka</a>
 * @version 1.0
 */
public class CharacterTranslationBuffer
{
    byte[]       input;
    int          offset;
    int          length;
    String       charset;
    int          complete;
    int          current;
    StringBuffer output;

    /**
     * Initialize for translation.
     * 
     * @param input Byte buffer that is to be translated
     * @param offset Starting offset in buffer. 0 is the beginning.
     * @param length Length of the buffer. 0 denotes an empty buffer.
     * @param charset Name of character used to encode characters.
     */
    public void load(byte input[], int offset, int length, String charset)
    {
        this.input = input;
        this.offset = offset;
        this.length = length;
        this.charset = charset;

        this.current = 0;
        this.complete = 0;
        this.output = new StringBuffer();
    }

    /**
     * Returns the next byte to process.
     */
    public byte next()
    {
        if (current < length)
        {
            return input[offset + current++];
        }
        else
            throw new IndexOutOfBoundsException();
    }

    /**
     * Backup current pointer or or more bytes.
     */
    public void backoff(int goBack)
    {
        current = current - goBack;
    }

    /**
     * Returns true if there are more bytes to read in the buffer.
     */
    public boolean hasNext()
    {
        return (current < length);
    }

    /**
     * Returns true if we have processed all bytes and the output is ready.
     */
    public boolean isComplete()
    {
        return complete >= length;
    }

    /**
     * Append a string to the output without affecting pending bytes.
     */
    public void append(String s)
    {
        output.append(s);
    }

    /**
     * Append a string to the output and clear pending bytes.
     */
    public void appendAndClearPending(String s)
    {
        output.append(s);
        complete = current;
    }

    /**
     * Translate pending bytes using selected character set. Bytes are
     * translated up to current position minus the backoff value.
     */
    public void translateAndAppendPending(int backoff)
            throws UnsupportedEncodingException
    {
        int bufferEnd = current - backoff;
        int len = bufferEnd - complete;
        output.append(new String(input, offset + complete, len, charset));
        complete = bufferEnd;
    }

    /**
     * Returns the output value as a Java String.
     */
    public String getOutput()
    {
        return output.toString();
    }
}