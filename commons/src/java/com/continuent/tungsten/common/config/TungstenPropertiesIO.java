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

package com.continuent.tungsten.common.config;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.Properties;
import java.util.TreeMap;

import com.continuent.tungsten.common.file.FileIO;
import com.continuent.tungsten.common.file.FileIOException;
import com.continuent.tungsten.common.file.FilePath;
import com.continuent.tungsten.common.file.JavaFileIO;

/**
 * This class reads and writes TungstenProperties data safely on a file system.
 * It uses the generic FileIO class to ensure that properties files are handled
 * as efficiently as possible.
 */
public class TungstenPropertiesIO
{
    /** Process file using JSON format serialization. */
    public static final String JSON            = "JSON";

    /** Process file using Java properties format serialization. */
    public static final String JAVA_PROPERTIES = "JAVA_PROPERTIES";

    // Properties with reasonable defaults.
    private String         format          = "JAVA_PROPERTIES";
    private String         charset         = "UTF-8";

    // Class to perform IO and location where to perform it.
    private final FileIO   fileIO;
    private final FilePath filePath;

    /**
     * Creates a new instance with user-specified FileIO implementation and file
     * path.
     */
    public TungstenPropertiesIO(FileIO fileIO, FilePath filePath)
    {
        this.fileIO = fileIO;
        this.filePath = filePath;
    }

    /**
     * Creates a new instance for OS file system operating on the
     * caller-specified file.
     */
    public TungstenPropertiesIO(File path)
    {
        this(new JavaFileIO(), new FilePath(path.getAbsolutePath()));
    }

    public String getFormat()
    {
        return format;
    }

    /** Sets the serialization format to use. */
    public void setFormat(String format)
    {
        this.format = format;
    }

    public String getCharset()
    {
        return charset;
    }

    /** Sets the character set to use. */
    public void setCharset(String charset)
    {
        this.charset = charset;
    }

    /** Returns true if the properties file exists. */
    public boolean exists()
    {
        return fileIO.exists(filePath);
    }

    /**
     * Delete the properties file.
     * 
     * @return true if fully successful, otherwise false.
     */
    public boolean delete()
    {
        return fileIO.delete(filePath, false);
    }

    /**
     * Write properties file to the file system using selected serialization
     * format and character set.
     * 
     * @param properties Properties to be written
     * @param fsync If true issue an fsync, otherwise just flush
     * @throws FileIOException Thrown if file is not writable
     */
    public void write(TungstenProperties properties, boolean fsync)
            throws FileIOException
    {
        String contents;
        if (JAVA_PROPERTIES.equals(format))
        {
            // Output into sorted properties format.
            TreeMap<String, String> map = new TreeMap<String, String>(
                    properties.map());
            StringBuffer sb = new StringBuffer();
            for (String key : map.keySet())
            {
                sb.append(String.format("%s=%s\n", key, map.get(key)));
            }
            contents = sb.toString();
        }
        else if (JSON.equals(format))
        {
            try
            {
                contents = properties.toJSON(true);
            }
            catch (Exception e)
            {
                throw new FileIOException("Unable to convert to JSON: file="
                        + filePath.toString() + " format=" + format, e);
            }
        }
        else
        {
            throw new FileIOException(
                    "Unrecognized property output format: file="
                            + filePath.toString() + " format=" + format);
        }

        // Write the results.
        fileIO.write(filePath, contents, charset, fsync);
    }

    /**
     * Read properties file from the file system using selected serialization
     * format and character set.
     * 
     * @return Properties instance
     * @throws FileIOException Thrown if file is not readable
     */
    public TungstenProperties read() throws FileIOException
    {
        // Read the file.
        String contents = fileIO.read(filePath, charset);

        // Deserialize using appropriate format.
        if (JAVA_PROPERTIES.equals(format))
        {
            StringReader sr = new StringReader(contents);
            Properties javaProps = new Properties();
            try
            {
                javaProps.load(sr);
            }
            catch (IOException e)
            {
                throw new FileIOException(
                        "Unable to read JSON properties: file="
                                + filePath.toString() + " format=" + format, e);
            }
            sr.close();
            TungstenProperties properties = new TungstenProperties();
            properties.load(javaProps);
            return properties;
        }
        else if (JSON.equals(format))
        {
            try
            {
                return TungstenProperties.loadFromJSON(contents);
            }
            catch (Exception e)
            {
                throw new FileIOException("Unable to convert to JSON: file="
                        + filePath.toString() + " format=" + format, e);
            }
        }
        else
        {
            throw new FileIOException(
                    "Unrecognized property input format: file="
                            + filePath.toString() + " format=" + format);
        }
    }
}