/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2013 Continuent Inc.
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

package com.continuent.tungsten.common.file;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

/**
 * Implements simple utility methods for storing and retrieving files located
 * under a base directory. This class is designed to support operations on both
 * standard Linux file systems as well as Hadoop. For this reason we use the
 * package-defined FileIOException as a covering exception for all underlying
 * exception types instead of exceptions like IOException that only apply to on
 * particular type of file system.
 */
public class FileIO
{
    /**
     * Internal filter class to select file names based on a prefix. If the
     * prefix value is null all names are selected.
     */
    public class LocalFilenameFilter implements FilenameFilter
    {
        private final String prefix;

        public LocalFilenameFilter(String prefix)
        {
            this.prefix = prefix;
        }

        public boolean accept(File dir, String name)
        {
            if (prefix == null)
                return true;
            else
                return name.startsWith(prefix);
        }
    }

    /** Returns true if path exists. */
    public boolean exists(FilePath path)
    {
        return new File(path.toString()).exists();
    }

    /** Returns true if path is an ordinary file. */
    public boolean isFile(FilePath path)
    {
        return new File(path.toString()).isFile();
    }

    /** Returns true if path is a directory. */
    public boolean isDirectory(FilePath path)
    {
        return new File(path.toString()).isDirectory();
    }

    /** Returns true if path is writable. */
    public boolean writable(FilePath path)
    {
        return new File(path.toString()).canWrite();
    }

    /** Returns true if path is readable. */
    public boolean readable(FilePath path)
    {
        return new File(path.toString()).canRead();
    }

    /**
     * Return a list of the names of children of this path.
     * 
     * @param path Path to search
     * @return An array of path names, which will be empty if there are no
     *         children
     */
    public String[] list(FilePath path)
    {
        return list(path, null);
    }

    /**
     * Return a list of the names of children of this path that start with the
     * given prefix.
     * 
     * @param path Path to search
     * @param prefix Required file name prefix or null to return all children
     * @return An array of path names, which will be empty if there are no
     *         children
     */
    public String[] list(FilePath path, String prefix)
    {
        // Children can only exist on directories.
        if (isDirectory(path))
        {
            File dir = new File(path.toString());
            String[] seqnoFileNames = dir.list(new LocalFilenameFilter(prefix));
            return seqnoFileNames;
        }
        else
        {
            return new String[0];
        }
    }

    /**
     * Create path as a new directory.
     * 
     * @param path Path to create
     * @return true if successful
     */
    public boolean mkdir(FilePath path)
    {
        return new File(path.toString()).mkdirs();
    }

    /**
     * Create path as a new directory including any intervening directories in
     * the path.
     * 
     * @param path Path to create
     * @return true if successful
     */
    public boolean mkdirs(FilePath path)
    {
        return new File(path.toString()).mkdirs();
    }

    /**
     * Delete path. This form ignored children.
     * 
     * @param path Path to delete
     * @return true if fully successful, otherwise false.
     */
    public boolean delete(FilePath path)
    {
        return delete(path, false);
    }

    /**
     * Delete path and optionally any children. Recursive deletes fail if we
     * cannot delete all children as well as the original path.
     * 
     * @param path Path to delete
     * @param recursive If true delete child files/directories as well
     * @return true if fully successful, otherwise false.
     */
    public boolean delete(FilePath path, boolean recursive)
    {
        // If the node does not exist, return immediately.
        if (!exists(path))
            return true;

        // Try to delete children if this is recursive. Otherwise,
        // we cannot continuent and must return.
        if (isDirectory(path))
        {
            for (String child : list(path))
            {
                if (recursive)
                {
                    boolean deleted = delete(new FilePath(path, child),
                            recursive);
                    if (!deleted)
                        return false;
                }
                else
                    return false;
            }
        }

        // Delete the path for which we were called.
        File fileToDelete = new File(path.toString());
        return fileToDelete.delete();
    }

    /**
     * Write data to file system using UTF-8 charset for file encoding and with
     * flush only.
     * 
     * @param path The file path
     * @param value The string to write in the file
     * @throws FileIOException Thrown if file is not writable
     */
    public void write(FilePath path, String value) throws FileIOException
    {
        write(path, value, "UTF-8", false);
    }

    /**
     * Write data to file system with flush only.
     * 
     * @param path The file path
     * @param value The string to write in the file
     * @param charset Character set of file data (e.g., UTF-8)
     * @throws FileIOException Thrown if file is not writable
     */
    public void write(FilePath path, String value, String charset)
            throws FileIOException
    {
        write(path, value, charset, false);
    }

    /**
     * Writes a string into a file, replacing an existing contents. There are
     * two durability options. If fsync is true, we issue the Java equivalent of
     * fsync, which is generally sufficient to survive a file system crash. If
     * fsync is false, we just flush, which will generally survive a process
     * crash.
     * 
     * @param path The file path
     * @param value The string to write in the file
     * @param charset Character set of file data (e.g., UTF-8)
     * @param fsync If true issue an fsync, otherwise just flush
     * @throws FileIOException Thrown if file is not writable
     */

    public void write(FilePath path, String value, String charset, boolean fsync)
            throws FileIOException
    {
        // Write the JSON and flush to storage. This overwrites any
        // previous version.
        FileOutputStream fos = null;
        try
        {
            File f = new File(path.toString());
            fos = new FileOutputStream(f);
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos,
                    charset));
            bw.write(value);
            bw.flush();
            try
            {
                bw.close();
            }
            catch (Exception e)
            {
            }
            if (fsync)
            {
                fos.getFD().sync();
            }
        }
        catch (IOException e)
        {
            throw new FileIOException("Unable to write data to file: file="
                    + path.toString() + " value=" + safeSynopsis(value, 20), e);
        }
        finally
        {
            if (fos != null)
            {
                try
                {
                    fos.close();
                }
                catch (IOException e)
                {
                }
            }
        }
    }

    // Print a prefix of a string value avoiding accidental index
    // out-of-bounds errors.
    private String safeSynopsis(String value, int length)
    {
        if (value.length() <= length)
            return value;
        else
            return value.substring(0, 10) + "...";
    }

    /**
     * Returns the value of the contents of a file as a string using UTF-8 as
     * charset encoding.
     * 
     * @param path The file path
     * @return Contents of the file, which is an empty string for a 0-length
     *         file
     * @throws FileIOException Thrown if file is not readable
     */
    public String read(FilePath path) throws FileIOException
    {
        return read(path, "UTF-8");
    }

    /**
     * Returns the value of the contents of a file as a string.
     * 
     * @param path The file path
     * @param charset Character set of file data (e.g., UTF-8)
     * @return Contents of the file, which is an empty string for a 0-length
     *         file
     * @throws FileIOException Thrown if file is not readable
     */
    public String read(FilePath path, String charset) throws FileIOException
    {
        // Read JSON from storage.
        FileInputStream fos = null;
        try
        {
            File f = new File(path.toString());
            fos = new FileInputStream(f);
            BufferedReader bf = new BufferedReader(new InputStreamReader(fos,
                    charset));
            StringBuffer buf = new StringBuffer();
            int nextChar = 0;
            while ((nextChar = bf.read()) > -1)
            {
                buf.append((char) nextChar);
            }
            return buf.toString();
        }
        catch (IOException e)
        {
            throw new FileIOException("Unable to read data from file: file="
                    + path.toString(), e);
        }
        finally
        {
            if (fos != null)
            {
                try
                {
                    fos.close();
                }
                catch (IOException e)
                {
                }
            }
        }
    }

}