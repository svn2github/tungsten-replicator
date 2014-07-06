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
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import com.continuent.tungsten.common.config.TungstenProperties;

/**
 * Implements FileIO operations for Hadoop Distributed File System (HDFS).
 */
public class HdfsFileIO implements FileIO
{
    // Variables used to access HDFS.
    private URI           uri;
    private FileSystem    hdfs;
    private Configuration hdfsConfig;

    /**
     * Internal filter class to select file names based on a prefix. If the
     * prefix value is null all names are selected.
     */
    public class LocalPathFilter implements PathFilter
    {
        private final String prefix;

        public LocalPathFilter(String prefix)
        {
            this.prefix = prefix;
        }

        public boolean accept(Path p)
        {
            if (prefix == null)
                return true;
            else
                return p.getName().startsWith(prefix);
        }
    }

    /**
     * Creates a new instance with connection to HDFS.
     * 
     * @param uri HDFS URI
     */
    public HdfsFileIO(URI uri, TungstenProperties props)
    {
        this.uri = uri;
        hdfsConfig = new Configuration();
        for (String key : props.keyNames())
        {
            String value = props.get(key);
            hdfsConfig.set(key, value);
        }
        try
        {
            this.hdfs = FileSystem.get(uri, hdfsConfig);
        }
        catch (IOException e)
        {
            throw new FileIOException("Unable to access HDFS: uri=" + uri
                    + " message=" + e.getMessage(), e);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.common.file.FileIO#exists(com.continuent.tungsten.common.file.FilePath)
     */
    @Override
    public boolean exists(FilePath path)
    {
        Path p = new Path(path.toString());
        try
        {
            return hdfs.exists(p);
        }
        catch (IOException e)
        {
            throw new FileIOException("Unable to check existence on HDFS: uri="
                    + uri + " path=" + p + " message=" + e.getMessage(), e);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.common.file.FileIO#isFile(com.continuent.tungsten.common.file.FilePath)
     */
    @Override
    public boolean isFile(FilePath path)
    {
        FileStatus fileStatus = getStatus(path);
        return fileStatus.isFile();
    }

    /**
     * Fetches a Hadoop file status instance for a particular path.
     */
    private FileStatus getStatus(FilePath path)
    {
        Path p = new Path(path.toString());
        try
        {
            return hdfs.getFileStatus(p);
        }
        catch (IOException e)
        {
            throw new FileIOException("Unable to fetch HDFS file status: uri="
                    + uri + " path=" + p + " message=" + e.getMessage(), e);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.common.file.FileIO#isDirectory(com.continuent.tungsten.common.file.FilePath)
     */
    @Override
    public boolean isDirectory(FilePath path)
    {
        FileStatus fileStatus = getStatus(path);
        return fileStatus.isDirectory();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.common.file.FileIO#writable(com.continuent.tungsten.common.file.FilePath)
     */
    @Override
    public boolean writable(FilePath path)
    {
        // TODO: Not sure how to get this for now.
        return exists(path);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.common.file.FileIO#readable(com.continuent.tungsten.common.file.FilePath)
     */
    @Override
    public boolean readable(FilePath path)
    {
        // TODO: Not sure how to get this for now.
        return exists(path);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.common.file.FileIO#list(com.continuent.tungsten.common.file.FilePath)
     */
    @Override
    public String[] list(FilePath path)
    {
        return list(path, null);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.common.file.FileIO#list(com.continuent.tungsten.common.file.FilePath,
     *      java.lang.String)
     */
    @Override
    public String[] list(FilePath path, String prefix)
    {
        // Children can only exist on directories.
        if (isDirectory(path))
        {
            Path p = new Path(path.toString());
            try
            {
                FileStatus[] statusArray = hdfs.listStatus(p,
                        new LocalPathFilter(prefix));
                String[] fileNames = new String[statusArray.length];
                for (int i = 0; i < statusArray.length; i++)
                {
                    fileNames[i] = statusArray[i].getPath().getName();
                }
                return fileNames;
            }
            catch (IOException e)
            {
                throw new FileIOException(
                        "Unable to check list directory on HDFS: uri=" + uri
                                + " path=" + p + " message=" + e.getMessage(),
                        e);
            }
        }
        else
        {
            return new String[0];
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.common.file.FileIO#mkdir(com.continuent.tungsten.common.file.FilePath)
     */
    @Override
    public boolean mkdir(FilePath path)
    {
        Path p = new Path(path.toString());
        try
        {
            return hdfs.mkdirs(p);
        }
        catch (IOException e)
        {
            throw new FileIOException(
                    "Unable to create directory on HDFS: uri=" + uri + " path="
                            + p + " message=" + e.getMessage(), e);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.common.file.FileIO#mkdirs(com.continuent.tungsten.common.file.FilePath)
     */
    @Override
    public boolean mkdirs(FilePath path)
    {
        // These operations are not distinct in HDFS.
        return mkdir(path);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.common.file.FileIO#delete(com.continuent.tungsten.common.file.FilePath)
     */
    @Override
    public boolean delete(FilePath path)
    {
        return delete(path, false);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.common.file.FileIO#delete(com.continuent.tungsten.common.file.FilePath,
     *      boolean)
     */
    @Override
    public boolean delete(FilePath path, boolean recursive)
    {
        // If the node does not exist, return immediately.
        if (!exists(path))
            return true;

        // Check for a non-recursive delete on a non-empty directory, which
        // cannot possibly succeed.
        if (!recursive && this.isDirectory(path) && this.list(path).length > 0)
        {
            return false;
        }

        // At this point the delete is either recursive or on a single file or
        // empty directory. It should succeed.
        Path p = new Path(path.toString());
        try
        {
            return hdfs.delete(p, recursive);
        }
        catch (IOException e)
        {
            throw new FileIOException("Unable to delete file on HDFS: uri="
                    + uri + " path=" + p + " message=" + e.getMessage(), e);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.common.file.FileIO#write(com.continuent.tungsten.common.file.FilePath,
     *      java.lang.String)
     */
    @Override
    public void write(FilePath path, String value) throws FileIOException
    {
        write(path, value, "UTF-8", true);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.common.file.FileIO#write(com.continuent.tungsten.common.file.FilePath,
     *      java.lang.String, java.lang.String)
     */
    @Override
    public void write(FilePath path, String value, String charset)
            throws FileIOException
    {
        write(path, value, charset, true);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.common.file.FileIO#write(com.continuent.tungsten.common.file.FilePath,
     *      java.lang.String, java.lang.String, boolean)
     */

    @Override
    public void write(FilePath path, String value, String charset, boolean fsync)
            throws FileIOException
    {
        // Write the data and flush to storage. This overwrites any
        // previous version.
        Path p = new Path(path.toString());
        FSDataOutputStream os = null;
        try
        {
            os = (FSDataOutputStream) this.getOutputStream(path);
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(os,
                    charset));
            bw.write(value);
            bw.flush();
            if (fsync)
            {
                os.hsync();
            }
        }
        catch (IOException e)
        {
            throw new FileIOException("Unable to write data to file: uri="
                    + uri + " path=" + p.toString() + " value="
                    + safeSynopsis(value, 20), e);
        }
        finally
        {
            if (os != null)
            {
                try
                {
                    os.close();
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
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.common.file.FileIO#read(com.continuent.tungsten.common.file.FilePath)
     */
    @Override
    public String read(FilePath path) throws FileIOException
    {
        return read(path, "UTF-8");
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.common.file.FileIO#read(com.continuent.tungsten.common.file.FilePath,
     *      java.lang.String)
     */
    @Override
    public String read(FilePath path, String charset) throws FileIOException
    {
        // Read data from storage.
        Path p = new Path(path.toString());
        FSDataInputStream fdos = null;
        try
        {
            fdos = (FSDataInputStream) this.getInputStream(path);
            BufferedReader bf = new BufferedReader(new InputStreamReader(fdos,
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
            throw new FileIOException("Unable to read data from file: uri="
                    + uri + " path=" + p, e);
        }
        finally
        {
            if (fdos != null)
            {
                try
                {
                    fdos.close();
                }
                catch (IOException e)
                {
                }
            }
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.common.file.FileIO#getInputStream(com.continuent.tungsten.common.file.FilePath)
     */
    public InputStream getInputStream(FilePath path) throws FileIOException
    {
        // Read data from storage.
        Path p = new Path(path.toString());
        try
        {
            return hdfs.open(p);
        }
        catch (IOException e)
        {
            throw new FileIOException("Unable to read data from file: uri="
                    + uri + " path=" + p, e);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.common.file.FileIO#getOutputStream(com.continuent.tungsten.common.file.FilePath)
     */
    public OutputStream getOutputStream(FilePath path) throws FileIOException
    {
        Path p = new Path(path.toString());
        try
        {
            return hdfs.create(p, true);
        }
        catch (IOException e)
        {
            throw new FileIOException("Unable to write data to file: uri="
                    + uri + " path=" + p.toString(), e);
        }
    }
}