/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2014 Continuent Inc.
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

package com.continuent.tungsten.common.io;

import java.io.IOException;
import java.io.InputStream;

/**
 * Encapsulates an InputStream in order to provide additional management of
 * stream capabilities. This class overrides all superclass methods to ensure a
 * faithful representation of the wrapped stream semantics.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class WrappedInputStream extends InputStream
{
    private final InputStream in;
    private final boolean     deterministic;

    /**
     * Creates a new wrapped input stream.
     * 
     * @param in InputStream instance to be wrapped
     * @param deterministic If true the stream gives accurate availability
     *            counts
     */
    public WrappedInputStream(InputStream in, boolean deterministic)
    {
        this.in = in;
        this.deterministic = deterministic;
    }

    /**
     * Returns true if {@link #available()} returns a number that can be trusted
     * for buffering.
     */
    public boolean isDeterministic()
    {
        return deterministic;
    }

    /**
     * @see java.io.InputStream#available()
     */
    public int available() throws IOException
    {
        return in.available();
    }

    /**
     * @see java.io.InputStream#close()
     */
    public void close() throws IOException
    {
        in.close();
    }

    /**
     * @see java.lang.Object#equals(java.lang.Object)
     */
    public boolean equals(Object obj)
    {
        return in.equals(obj);
    }

    /**
     * @see java.lang.Object#hashCode()
     */
    public int hashCode()
    {
        return in.hashCode();
    }

    /**
     * @see java.io.InputStream#mark(int)
     */
    public void mark(int arg0)
    {
        in.mark(arg0);
    }

    /**
     * @see java.io.InputStream#markSupported()
     */
    public boolean markSupported()
    {
        return in.markSupported();
    }

    /**
     * @see java.io.InputStream#read()
     */
    public int read() throws IOException
    {
        return in.read();
    }

    /**
     * @see java.io.InputStream#read(byte[], int, int)
     */
    public int read(byte[] arg0, int arg1, int arg2) throws IOException
    {
        return in.read(arg0, arg1, arg2);
    }

    /**
     * @see java.io.InputStream#read(byte[])
     */
    public int read(byte[] arg0) throws IOException
    {
        return in.read(arg0);
    }

    /**
     * @see java.io.InputStream#reset()
     */
    public void reset() throws IOException
    {
        in.reset();
    }

    /**
     * @see java.io.InputStream#skip(long)
     */
    public long skip(long arg0) throws IOException
    {
        return in.skip(arg0);
    }

    /**
     * @see java.lang.Object#toString()
     */
    public String toString()
    {
        return in.toString();
    }
}