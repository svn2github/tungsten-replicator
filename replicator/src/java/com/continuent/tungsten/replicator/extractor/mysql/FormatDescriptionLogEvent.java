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
 * Initial developer(s): Seppo Jaakola
 * Contributor(s): Stephane Giron
 */

package com.continuent.tungsten.replicator.extractor.mysql;

import com.continuent.tungsten.replicator.ReplicatorException;

/**
 * @author <a href="mailto:seppo.jaakola@continuent.com">Seppo Jaakola</a>
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class FormatDescriptionLogEvent extends StartLogEvent
{
    protected int  binlogVersion;
    public short   commonHeaderLength;
    public short[] postHeaderLength;
    private int    eventTypesCount;

    public FormatDescriptionLogEvent(byte[] buffer, int eventLength,
            FormatDescriptionLogEvent descriptionEvent)
            throws ReplicatorException
    {
        super(buffer, descriptionEvent);

        if (logger.isDebugEnabled())
            logger.debug("FormatDescriptionLogEvent");

        commonHeaderLength = buffer[MysqlBinlog.LOG_EVENT_MINIMAL_HEADER_LEN
                + MysqlBinlog.ST_COMMON_HEADER_LEN_OFFSET];

        if (commonHeaderLength < MysqlBinlog.OLD_HEADER_LEN)
        {
            throw new MySQLExtractException(
                    "Format Description event header length is too short");
        }
        
        eventTypesCount = eventLength
                - (MysqlBinlog.LOG_EVENT_MINIMAL_HEADER_LEN
                        + MysqlBinlog.ST_COMMON_HEADER_LEN_OFFSET + 1);

        if (logger.isDebugEnabled())
            logger.debug("commonHeaderLength= " + commonHeaderLength
                    + " eventTypesCount= " + eventTypesCount);

    }

    public FormatDescriptionLogEvent(int binlogVersion)
    {
        this.binlogVersion = binlogVersion;
        postHeaderLength = new short[MysqlBinlog.ENUM_END_EVENT];

        /* identify binlog format */
        switch (binlogVersion)
        {
            case 1 : // 3.23
                commonHeaderLength = MysqlBinlog.OLD_HEADER_LEN;
                eventTypesCount = MysqlBinlog.FORMAT_DESCRIPTION_EVENT - 1;

                postHeaderLength[MysqlBinlog.START_EVENT_V3 - 1] = MysqlBinlog.START_V3_HEADER_LEN;
                postHeaderLength[MysqlBinlog.QUERY_EVENT - 1] = MysqlBinlog.QUERY_HEADER_MINIMAL_LEN;
                postHeaderLength[MysqlBinlog.STOP_EVENT - 1] = 0;
                postHeaderLength[MysqlBinlog.ROTATE_EVENT - 1] = 0;
                postHeaderLength[MysqlBinlog.INTVAR_EVENT - 1] = 0;
                postHeaderLength[MysqlBinlog.LOAD_EVENT - 1] = MysqlBinlog.LOAD_HEADER_LEN;
                postHeaderLength[MysqlBinlog.SLAVE_EVENT - 1] = 0;
                postHeaderLength[MysqlBinlog.CREATE_FILE_EVENT - 1] = MysqlBinlog.CREATE_FILE_HEADER_LEN;
                postHeaderLength[MysqlBinlog.APPEND_BLOCK_EVENT - 1] = MysqlBinlog.APPEND_BLOCK_HEADER_LEN;
                postHeaderLength[MysqlBinlog.EXEC_LOAD_EVENT - 1] = MysqlBinlog.EXEC_LOAD_HEADER_LEN;
                postHeaderLength[MysqlBinlog.DELETE_FILE_EVENT - 1] = MysqlBinlog.DELETE_FILE_HEADER_LEN;
                postHeaderLength[MysqlBinlog.NEW_LOAD_EVENT - 1] = postHeaderLength[MysqlBinlog.LOAD_EVENT - 1];
                postHeaderLength[MysqlBinlog.RAND_EVENT - 1] = 0;
                postHeaderLength[MysqlBinlog.USER_VAR_EVENT - 1] = 0;
                break;
            case 3 : // 4.0.2
                commonHeaderLength = MysqlBinlog.OLD_HEADER_LEN;
                eventTypesCount = MysqlBinlog.FORMAT_DESCRIPTION_EVENT - 1;

                postHeaderLength[MysqlBinlog.START_EVENT_V3 - 1] = MysqlBinlog.START_V3_HEADER_LEN;
                postHeaderLength[MysqlBinlog.QUERY_EVENT - 1] = MysqlBinlog.QUERY_HEADER_MINIMAL_LEN;
                postHeaderLength[MysqlBinlog.STOP_EVENT - 1] = 0;
                postHeaderLength[MysqlBinlog.ROTATE_EVENT - 1] = MysqlBinlog.ROTATE_HEADER_LEN;
                postHeaderLength[MysqlBinlog.INTVAR_EVENT - 1] = 0;
                postHeaderLength[MysqlBinlog.LOAD_EVENT - 1] = MysqlBinlog.LOAD_HEADER_LEN;
                postHeaderLength[MysqlBinlog.SLAVE_EVENT - 1] = 0;
                postHeaderLength[MysqlBinlog.CREATE_FILE_EVENT - 1] = MysqlBinlog.CREATE_FILE_HEADER_LEN;
                postHeaderLength[MysqlBinlog.APPEND_BLOCK_EVENT - 1] = MysqlBinlog.APPEND_BLOCK_HEADER_LEN;
                postHeaderLength[MysqlBinlog.EXEC_LOAD_EVENT - 1] = MysqlBinlog.EXEC_LOAD_HEADER_LEN;
                postHeaderLength[MysqlBinlog.DELETE_FILE_EVENT - 1] = MysqlBinlog.DELETE_FILE_HEADER_LEN;
                postHeaderLength[MysqlBinlog.NEW_LOAD_EVENT - 1] = postHeaderLength[MysqlBinlog.LOAD_EVENT - 1];
                postHeaderLength[MysqlBinlog.RAND_EVENT - 1] = 0;
                postHeaderLength[MysqlBinlog.USER_VAR_EVENT - 1] = 0;

                break;
            case 4 : // 5.0
                commonHeaderLength = MysqlBinlog.LOG_EVENT_HEADER_LEN;
                eventTypesCount = MysqlBinlog.LOG_EVENT_TYPES;

                postHeaderLength[MysqlBinlog.START_EVENT_V3 - 1] = MysqlBinlog.START_V3_HEADER_LEN;
                postHeaderLength[MysqlBinlog.QUERY_EVENT - 1] = MysqlBinlog.QUERY_HEADER_LEN;
                postHeaderLength[MysqlBinlog.ROTATE_EVENT - 1] = MysqlBinlog.ROTATE_HEADER_LEN;
                postHeaderLength[MysqlBinlog.LOAD_EVENT - 1] = MysqlBinlog.LOAD_HEADER_LEN;
                postHeaderLength[MysqlBinlog.CREATE_FILE_EVENT - 1] = MysqlBinlog.CREATE_FILE_HEADER_LEN;
                postHeaderLength[MysqlBinlog.APPEND_BLOCK_EVENT - 1] = MysqlBinlog.APPEND_BLOCK_HEADER_LEN;
                postHeaderLength[MysqlBinlog.EXEC_LOAD_EVENT - 1] = MysqlBinlog.EXEC_LOAD_HEADER_LEN;
                postHeaderLength[MysqlBinlog.DELETE_FILE_EVENT - 1] = MysqlBinlog.DELETE_FILE_HEADER_LEN;
                postHeaderLength[MysqlBinlog.NEW_LOAD_EVENT - 1] = postHeaderLength[MysqlBinlog.LOAD_EVENT - 1];
                postHeaderLength[MysqlBinlog.FORMAT_DESCRIPTION_EVENT - 1] = MysqlBinlog.FORMAT_DESCRIPTION_HEADER_LEN;
                postHeaderLength[MysqlBinlog.TABLE_MAP_EVENT - 1] = MysqlBinlog.TABLE_MAP_HEADER_LEN;
                postHeaderLength[MysqlBinlog.WRITE_ROWS_EVENT - 1] = MysqlBinlog.ROWS_HEADER_LEN;
                postHeaderLength[MysqlBinlog.UPDATE_ROWS_EVENT - 1] = MysqlBinlog.ROWS_HEADER_LEN;
                postHeaderLength[MysqlBinlog.DELETE_ROWS_EVENT - 1] = MysqlBinlog.ROWS_HEADER_LEN;
                postHeaderLength[MysqlBinlog.EXECUTE_LOAD_QUERY_EVENT - 1] = MysqlBinlog.EXECUTE_LOAD_QUERY_HEADER_LEN;
                postHeaderLength[MysqlBinlog.APPEND_BLOCK_EVENT - 1] = MysqlBinlog.APPEND_BLOCK_HEADER_LEN;                
                postHeaderLength[MysqlBinlog.DELETE_FILE_EVENT - 1] = MysqlBinlog.DELETE_FILE_HEADER_LEN;
                break;
        }
    }
}
