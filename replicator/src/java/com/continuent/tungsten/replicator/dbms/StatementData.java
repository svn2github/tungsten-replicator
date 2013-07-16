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
 * Initial developer(s): Teemu Ollakka
 * Contributor(s): Robert Hodges, Stephane Giron
 */

package com.continuent.tungsten.replicator.dbms;

import java.io.UnsupportedEncodingException;

import com.continuent.tungsten.replicator.event.ReplOptionParams;

/**
 * Defines a SQL statement that must be replicated.
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public class StatementData extends DBMSData
{
    private static final long  serialVersionUID  = 1L;

    public static final String CREATE_OR_DROP_DB = "createOrDropDB";

    private String             defaultSchema;
    private Long               timestamp;

    private String             query;
    private byte[]             queryAsBytes;

    // Internal buffer used to return string values. This value is not
    // serialized.
    private transient String   queryAsBytesTranslated;
    
    // Transient SQL parsing metata stored here to avoid later reparsing.
    private transient Object   metadata;


    private int                errorCode;

    public StatementData(String query)
    {
        super();
        this.defaultSchema = null;
        this.timestamp = null;
        this.query = query;
    }

    /**
     * Creates a new instance including timestamp and default schema.
     */
    public StatementData(String query, Long timestamp, String defaultSchema)
    {
        this.defaultSchema = defaultSchema;
        this.timestamp = timestamp;
        this.query = query;
    }

    /**
     * Returns the default schema or null if default schema should be inferred
     * from a previous SqlStatement instance in the same transaction.
     */
    public String getDefaultSchema()
    {
        return defaultSchema;
    }

    /**
     * Returns the current timestamp in order to be able to process values that
     * refer to current time or a null if timestamp is not relevant to this
     * query.
     */
    public Long getTimestamp()
    {
        return timestamp;
    }

    /**
     * Returns the SQL statement that must be replicated.
     */
    public String getQuery()
    {
        if (this.queryAsBytes == null)
            return query;
        else
        {
            if (this.queryAsBytesTranslated == null)
            {
                // If we need a string and don't have it, we use a temporary
                // translation buffer. If we know the byte character set, we
                // translate faithfully. If we don't know or the encoding is
                // unknown, we fall back to the platform character set.
                String charsetName = getOption(ReplOptionParams.JAVA_CHARSET_NAME);
                if (charsetName == null)
                    queryAsBytesTranslated = new String(queryAsBytes);
                else
                {
                    try
                    {
                        queryAsBytesTranslated = new String(queryAsBytes,
                                charsetName);
                    }
                    catch (UnsupportedEncodingException e)
                    {
                        queryAsBytesTranslated = new String(queryAsBytes);
                    }
                }
            }
            return queryAsBytesTranslated;
        }
    }

    public void setDefaultSchema(String defaultSchema)
    {
        this.defaultSchema = defaultSchema;
    }

    public void setTimestamp(Long timestamp)
    {
        this.timestamp = timestamp;
    }

    public void setQuery(String query)
    {
        this.query = query;
        this.queryAsBytes = null;
    }

    public void setQuery(byte[] query)
    {
        this.queryAsBytes = query;
        this.query = null;
    }

    /**
     * Append to the query, translating if necessary to a byte buffer if the
     * query is stored in bytes.
     * 
     * @param buffer String value to be appended.
     */
    public void appendToQuery(String buffer)
    {
        if (this.queryAsBytes == null)
            query = query + buffer;
        else
        {
            String charset = getCharset();
            byte[] appendBuffer;
            if (charset == null)
                appendBuffer = buffer.getBytes();
            else
            {
                try
                {
                    appendBuffer = buffer.getBytes(charset);
                }
                catch (UnsupportedEncodingException e)
                {
                    appendBuffer = buffer.getBytes();
                }
            }
            byte[] buf = new byte[queryAsBytes.length + appendBuffer.length];
            System.arraycopy(queryAsBytes, 0, buf, 0, queryAsBytes.length);
            System.arraycopy(appendBuffer, 0, buf, queryAsBytes.length,
                    appendBuffer.length);
            queryAsBytes = buf;
            queryAsBytesTranslated = null;
        }
    }


    /**
     * Returns the Java character set name of the statement as represented in
     * bytes or null if not known.
     */
    public String getCharset()
    {
        return getOption(ReplOptionParams.JAVA_CHARSET_NAME);
    }

    /**
     * Sets the character set name for this statement as represented in bytes.
     * 
     * @param charset Java character set name
     */
    public void setCharset(String charset)
    {
        addOption(ReplOptionParams.JAVA_CHARSET_NAME, charset);
    }

    @Override
    public String toString()
    {
        String toStringValue = getQuery();
        if (toStringValue.length() > 1000)
            return toStringValue.substring(0, 999);
        else
            return toStringValue;
    }

    public void setErrorCode(int errorCode)
    {
        this.errorCode = errorCode;
    }

    public int getErrorCode()
    {
        return errorCode;
    }

    public byte[] getQueryAsBytes()
    {
        return queryAsBytes;
    }
    
    public Object getParsingMetadata()
    {
        return this.metadata;
    }
    
    public void setParsingMetadata(Object o)
    {
       this.metadata = o;
    }
}
