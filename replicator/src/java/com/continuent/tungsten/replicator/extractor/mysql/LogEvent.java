/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2009-2013 Continuent Inc.
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

import java.io.EOFException;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntime;
import com.continuent.tungsten.replicator.extractor.ExtractorException;
import com.continuent.tungsten.replicator.extractor.mysql.conversion.BigEndianConversion;
import com.continuent.tungsten.replicator.extractor.mysql.conversion.LittleEndianConversion;

/**
 * @author <a href="mailto:seppo.jaakola@continuent.com">Seppo Jaakola</a>
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */

public abstract class LogEvent
{
    protected static Logger logger              = Logger.getLogger(LogEvent.class);

    protected long          execTime;
    protected int           type;
    protected Timestamp     when;
    protected int           serverId;

    protected int           logPos;
    protected int           flags;

    protected boolean       threadSpecificEvent = false;

    protected String        startPosition       = "";

    public LogEvent()
    {
        type = MysqlBinlog.START_EVENT_V3;
    }

    public LogEvent(byte[] buffer, FormatDescriptionLogEvent descriptionEvent,
            int eventType) throws ReplicatorException
    {
        type = eventType;

        try
        {
            when = new Timestamp(
                    1000 * LittleEndianConversion
                            .convert4BytesToLong(buffer, 0));
            serverId = (int) LittleEndianConversion.convert4BytesToLong(buffer,
                    MysqlBinlog.SERVER_ID_OFFSET);
            if (descriptionEvent.binlogVersion == 1)
            {
                logPos = 0;
                flags = 0;
                return;
            }

            /* 4.0 or newer */
            logPos = (int) LittleEndianConversion.convert4BytesToLong(buffer,
                    MysqlBinlog.LOG_POS_OFFSET);
            /*
             * If the log is 4.0 (so here it can only be a 4.0 relay log read by
             * the SQL thread or a 4.0 master binlog read by the I/O thread),
             * log_pos is the beginning of the event: we transform it into the
             * end of the event, which is more useful. But how do you know that
             * the log is 4.0: you know it if description_event is version 3and
             * you are not reading a Format_desc (remember that mysqlbinlog
             * starts by assuming that 5.0 logs are in 4.0 format, until it
             * finds a Format_desc).
             */

            if ((descriptionEvent.binlogVersion == 3)
                    && (buffer[MysqlBinlog.EVENT_TYPE_OFFSET] < MysqlBinlog.FORMAT_DESCRIPTION_EVENT)
                    && (logPos > 0))
            {
                /*
                 * If log_pos=0, don't change it. log_pos==0 is a marker to mean
                 * "don't change rli->group_master_log_pos" (see
                 * inc_group_relay_log_pos()). As it is unreal log_pos, adding
                 * the event len's is nonsense. For example, a fake Rotate event
                 * should not have its log_pos (which is 0) changed or it will
                 * modify Exec_master_log_pos in SHOW SLAVE STATUS, displaying a
                 * nonsense value of (a non-zero offset which does not exist in
                 * the master's binlog, so which will cause problems if the user
                 * uses this value in CHANGE MASTER).
                 */
                logPos += LittleEndianConversion.convert4BytesToLong(buffer,
                        MysqlBinlog.EVENT_LEN_OFFSET);
            }
            if (logger.isDebugEnabled())
                logger.debug("log_pos: " + logPos);

            flags = LittleEndianConversion.convert2BytesToInt(buffer,
                    MysqlBinlog.FLAGS_OFFSET);
            /*
             * TODO LOG_EVENT_THREAD_SPECIFIC_F = 0x4 (New in 4.1.0) Used only
             * by mysqlbinlog (not by the replication code at all) to be able to
             * deal properly with temporary tables. mysqlbinlog displays events
             * from the binary log in printable format, so that you can feed the
             * output into mysql (the command-line interpreter), to achieve
             * incremental backup recovery.
             */
            threadSpecificEvent = ((flags & MysqlBinlog.LOG_EVENT_THREAD_SPECIFIC_F) == MysqlBinlog.LOG_EVENT_THREAD_SPECIFIC_F);
            if (logger.isDebugEnabled())
                logger.debug("Event is thread-specific = "
                        + threadSpecificEvent);

            if ((buffer[MysqlBinlog.EVENT_TYPE_OFFSET] == MysqlBinlog.FORMAT_DESCRIPTION_EVENT)
                    || (buffer[MysqlBinlog.EVENT_TYPE_OFFSET] == MysqlBinlog.ROTATE_EVENT))
            {
                /*
                 * These events always have a header which stops here (i.e.
                 * their header is FROZEN).
                 */
                /*
                 * Initialization to zero of all other Log_event members as
                 * they're not specified. Currently there are no such members;
                 * in the future there will be an event UID (but
                 * Format_description and Rotate don't need this UID, as they
                 * are not propagated through --log-slave-updates (remember the
                 * UID is used to not play a query twice when you have two
                 * masters which are slaves of a 3rd master). Then we are done.
                 */
                return;
            }
            /*
             * otherwise, go on with reading the header from buffer (nothing for
             * now)
             */
        }
        catch (IOException e)
        {
            throw new MySQLExtractException("log event create failed", e);
        }
    }

    public long getExecTime()
    {
        return execTime;
    }

    public Timestamp getWhen()
    {
        return when;
    }

    /**
     * Returns the position for the next event.
     * 
     * @return Returns the logPos.
     */
    public int getNextEventPosition()
    {
        return logPos;
    }

    private static LogEvent readLogEvent(boolean parseStatements,
            String currentPosition, byte[] buffer, int eventLength,
            FormatDescriptionLogEvent descriptionEvent,
            boolean useBytesForString) throws ReplicatorException
    {
        LogEvent event = null;

        switch (buffer[MysqlBinlog.EVENT_TYPE_OFFSET])
        {
            case MysqlBinlog.QUERY_EVENT :
                event = new QueryLogEvent(buffer, eventLength,
                        descriptionEvent, parseStatements, useBytesForString,
                        currentPosition);
                break;
            case MysqlBinlog.LOAD_EVENT :
                logger.warn("Skipping unsupported LOAD_EVENT");
                break;
            case MysqlBinlog.NEW_LOAD_EVENT :
                logger.warn("Skipping unsupported NEW_LOAD_EVENT");
                break;
            case MysqlBinlog.ROTATE_EVENT :
                event = new RotateLogEvent(buffer, eventLength,
                        descriptionEvent, currentPosition);
                break;
            case MysqlBinlog.SLAVE_EVENT : /* can never happen (unused event) */
                logger.warn("Skipping unsupported SLAVE_EVENT");
                break;
            case MysqlBinlog.CREATE_FILE_EVENT :
                logger.warn("Skipping unsupported CREATE_FILE_EVENT");
                break;
            case MysqlBinlog.APPEND_BLOCK_EVENT :
                if (logger.isDebugEnabled())
                    logger.debug("reading APPEND_BLOCK_EVENT");
                event = new AppendBlockLogEvent(buffer, eventLength,
                        descriptionEvent, currentPosition);
                break;
            case MysqlBinlog.DELETE_FILE_EVENT :
                if (logger.isDebugEnabled())
                    logger.debug("reading DELETE_FILE_EVENT");
                event = new DeleteFileLogEvent(buffer, eventLength,
                        descriptionEvent, currentPosition);
                break;
            case MysqlBinlog.EXEC_LOAD_EVENT :
                logger.warn("Skipping unsupported EXEC_LOAD_EVENT");
                break;
            case MysqlBinlog.START_EVENT_V3 :
                /* this is sent only by MySQL <=4.x */
                logger.warn("Skipping unsupported START_EVENT_V3");
                break;
            case MysqlBinlog.STOP_EVENT :
                event = new StopLogEvent(buffer, eventLength, descriptionEvent,
                        currentPosition);
                break;
            case MysqlBinlog.INTVAR_EVENT :
                if (logger.isDebugEnabled())
                    logger.debug("extracting INTVAR_EVENT");
                event = new IntvarLogEvent(buffer, eventLength,
                        descriptionEvent, currentPosition);
                break;
            case MysqlBinlog.XID_EVENT :
                event = new XidLogEvent(buffer, eventLength, descriptionEvent,
                        currentPosition);
                break;
            case MysqlBinlog.RAND_EVENT :
                event = new RandLogEvent(buffer, eventLength, descriptionEvent,
                        currentPosition);
                break;
            case MysqlBinlog.USER_VAR_EVENT :
                event = new UserVarLogEvent(buffer, eventLength,
                        descriptionEvent, currentPosition);
                break;
            case MysqlBinlog.FORMAT_DESCRIPTION_EVENT :
                event = new FormatDescriptionLogEvent(buffer, eventLength,
                        descriptionEvent, currentPosition);
                break;
            case MysqlBinlog.PRE_GA_WRITE_ROWS_EVENT :
                logger.warn("Skipping unsupported PRE_GA_WRITE_ROWS_EVENT");
                break;
            case MysqlBinlog.PRE_GA_UPDATE_ROWS_EVENT :
                logger.warn("Skipping unsupported PRE_GA_UPDATE_ROWS_EVENT");
                break;
            case MysqlBinlog.PRE_GA_DELETE_ROWS_EVENT :
                logger.warn("Skipping unsupported PRE_GA_DELETE_ROWS_EVENT");
                break;
            case MysqlBinlog.WRITE_ROWS_EVENT :
            case MysqlBinlog.NEW_WRITE_ROWS_EVENT :
                if (logger.isDebugEnabled())
                    logger.debug("reading WRITE_ROWS_EVENT");
                event = new WriteRowsLogEvent(buffer, eventLength,
                        descriptionEvent, useBytesForString, currentPosition);
                break;
            case MysqlBinlog.UPDATE_ROWS_EVENT :
            case MysqlBinlog.NEW_UPDATE_ROWS_EVENT :
                if (logger.isDebugEnabled())
                    logger.debug("reading UPDATE_ROWS_EVENT");
                event = new UpdateRowsLogEvent(buffer, eventLength,
                        descriptionEvent, useBytesForString, currentPosition);
                break;
            case MysqlBinlog.DELETE_ROWS_EVENT :
            case MysqlBinlog.NEW_DELETE_ROWS_EVENT :
                if (logger.isDebugEnabled())
                    logger.debug("reading DELETE_ROWS_EVENT");
                event = new DeleteRowsLogEvent(buffer, eventLength,
                        descriptionEvent, useBytesForString, currentPosition);
                break;
            case MysqlBinlog.TABLE_MAP_EVENT :
                if (logger.isDebugEnabled())
                    logger.debug("reading TABLE_MAP_EVENT");
                event = new TableMapLogEvent(buffer, eventLength,
                        descriptionEvent, currentPosition);
                break;
            case MysqlBinlog.BEGIN_LOAD_QUERY_EVENT :
                if (logger.isDebugEnabled())
                    logger.debug("reading BEGIN_LOAD_QUERY_EVENT");
                event = new BeginLoadQueryLogEvent(buffer, eventLength,
                        descriptionEvent, currentPosition);
                break;
            case MysqlBinlog.EXECUTE_LOAD_QUERY_EVENT :
                if (logger.isDebugEnabled())
                    logger.debug("reading EXECUTE_LOAD_QUERY_EVENT");
                event = new ExecuteLoadQueryLogEvent(buffer, eventLength,
                        descriptionEvent, parseStatements, currentPosition);
                break;
            case MysqlBinlog.INCIDENT_EVENT :
                logger.warn("Skipping unsupported INCIDENT_EVENT");
                break;
            default :
                logger.warn("Skipping unrecognized binlog event type "
                        + buffer[MysqlBinlog.EVENT_TYPE_OFFSET]);
        }
        return event;
    }

    public static LogEvent readLogEvent(ReplicatorRuntime runtime,
            BinlogReader position, FormatDescriptionLogEvent descriptionEvent,
            boolean parseStatements, boolean useBytesForString,
            boolean prefetchSchemaNameLDI) throws ReplicatorException,
            InterruptedException
    {
        int eventLength = 0;
        byte[] header = new byte[descriptionEvent.commonHeaderLength];

        try
        {
            String currentPosition = position.toString();

            // read the header part
            // timeout is set to 2 minutes.
            readDataFromBinlog(runtime, position, header, 0, header.length, 120);

            // Extract event length
            eventLength = (int) LittleEndianConversion.convert4BytesToLong(
                    header, MysqlBinlog.EVENT_LEN_OFFSET);

            eventLength -= header.length;

            byte[] fullEvent = new byte[header.length + eventLength];

            // read the event data part
            // timeout is set to 2 minutes
            readDataFromBinlog(runtime, position, fullEvent, header.length,
                    eventLength, 120);

            System.arraycopy(header, 0, fullEvent, 0, header.length);

            LogEvent event = readLogEvent(parseStatements, currentPosition,
                    fullEvent, fullEvent.length, descriptionEvent,
                    useBytesForString);

            // If schema name has to be prefetched, check if it is a BEGIN LOAD
            // EVENT
            if (prefetchSchemaNameLDI
                    && event instanceof BeginLoadQueryLogEvent)
            {
                if (logger.isDebugEnabled())
                    logger.debug("Got Begin Load Query Event - Looking for corresponding Execute Event");

                BeginLoadQueryLogEvent beginLoadEvent = (BeginLoadQueryLogEvent) event;
                // Spawn a new data input stream
                BinlogReader tempPosition = position.clone();
                tempPosition.setEventID(position.getEventID() + 1);
                tempPosition.open();

                if (logger.isDebugEnabled())
                    logger.debug("Reading from " + tempPosition);
                boolean found = false;
                byte[] tmpHeader = new byte[descriptionEvent.commonHeaderLength];

                String tempPos;
                while (!found)
                {
                    tempPos = tempPosition.toString();
                    readDataFromBinlog(runtime, tempPosition, tmpHeader, 0,
                            tmpHeader.length, 60);

                    // Extract event length
                    eventLength = (int) LittleEndianConversion
                            .convert4BytesToLong(tmpHeader,
                                    MysqlBinlog.EVENT_LEN_OFFSET)
                            - tmpHeader.length;

                    if (tmpHeader[MysqlBinlog.EVENT_TYPE_OFFSET] == MysqlBinlog.EXECUTE_LOAD_QUERY_EVENT)
                    {
                        fullEvent = new byte[tmpHeader.length + eventLength];
                        readDataFromBinlog(runtime, tempPosition, fullEvent,
                                tmpHeader.length, eventLength, 120);

                        System.arraycopy(tmpHeader, 0, fullEvent, 0,
                                tmpHeader.length);

                        LogEvent tempEvent = readLogEvent(parseStatements,
                                tempPos, fullEvent, fullEvent.length,
                                descriptionEvent, useBytesForString);

                        if (tempEvent instanceof ExecuteLoadQueryLogEvent)
                        {
                            ExecuteLoadQueryLogEvent execLoadQueryEvent = (ExecuteLoadQueryLogEvent) tempEvent;
                            if (execLoadQueryEvent.getFileID() == beginLoadEvent
                                    .getFileID())
                            {
                                if (logger.isDebugEnabled())
                                    logger.debug("Found corresponding Execute Load Query Event - Schema is "
                                            + execLoadQueryEvent.getDefaultDb());
                                beginLoadEvent.setSchemaName(execLoadQueryEvent
                                        .getDefaultDb());
                                found = true;
                            }
                        }

                    }
                    else
                    {
                        long skip = 0;
                        while (skip != eventLength)
                        {
                            skip += tempPosition.skip(eventLength - skip);
                        }
                    }
                }
                // Release the file handler
                tempPosition.close();
            }

            return event;
        }
        catch (EOFException e)
        {
            throw new MySQLExtractException("EOFException while reading "
                    + eventLength + " bytes from binlog ", e);
        }
        catch (IOException e)
        {
            throw new MySQLExtractException("binlog read error", e);
        }
    }

    /**
     * readDataFromBinlog waits for data to be fully written in the binlog file
     * and then reads it.
     * 
     * @param runtime replicator runtime
     * @param dis Input stream from which data will be read
     * @param data Array of byte that will contain read data
     * @param offset Position in the previous array where data should be written
     * @param length Data length to be read
     * @param timeout Maximum time to wait for data to be available
     * @throws IOException if an error occurs while reading from the stream
     * @throws ReplicatorException if the timeout is reached
     */
    private static void readDataFromBinlog(ReplicatorRuntime runtime,
            BinlogReader binlog, byte[] data, int offset, int length,
            int timeout) throws IOException, ReplicatorException,
            InterruptedException
    {
        boolean alreadyLogged = false;
        int spentTime = 0;
        int timeoutInMs = timeout * 1000;

        long available;
        while ((available = binlog.available()) < (long) length)
        {
            if (!alreadyLogged)
            {
                if (logger.isDebugEnabled())
                {
                    // This conditions appears commonly on slow file systems,
                    // hence should be a debug message.
                    logger.debug("Trying to read more bytes (" + length
                            + ") than available in the file (" + available
                            + " in " + binlog.getFileName()
                            + ")... waiting for data to be available");
                }
                alreadyLogged = true;
            }

            try
            {
                if (spentTime < timeoutInMs)
                {
                    Thread.sleep(1);
                    spentTime++;
                }
                else
                    throw new MySQLExtractException(
                            "Timeout while waiting for data : spent more than "
                                    + timeout + " seconds while waiting for "
                                    + length + " bytes to be available");
            }
            catch (InterruptedException e)
            {
            }
        }
        binlog.read(data, offset, length);
    }

    public int getType()
    {
        return type;
    }

    protected static String hexdump(byte[] buffer, int offset)
    {
        StringBuffer dump = new StringBuffer();
        if ((buffer.length - offset) > 0)
        {
            dump.append(String.format("%02x", buffer[offset]));
            for (int i = offset + 1; i < buffer.length; i++)
            {
                dump.append("_");
                dump.append(String.format("%02x", buffer[i]));
            }
        }
        return dump.toString();
    }

    protected String hexdump(byte[] buffer, int offset, int length)
    {
        StringBuffer dump = new StringBuffer();

        if (buffer.length >= offset + length)
        {
            dump.append(String.format("%02x", buffer[offset]));
            for (int i = offset + 1; i < offset + length; i++)
            {
                dump.append("_");
                dump.append(String.format("%02x", buffer[i]));
            }
        }
        return dump.toString();
    }

    protected void doChecksum(byte[] buffer, int eventLength,
            FormatDescriptionLogEvent descriptionEvent)
            throws ExtractorException
    {
        if (descriptionEvent.useChecksum())
        {
            long checksum = MysqlBinlog.getChecksum(
                    descriptionEvent.getChecksumAlgo(), buffer, 0, eventLength);
            if (checksum > -1)
                try
                {
                    long binlogChecksum = LittleEndianConversion
                            .convert4BytesToLong(buffer, eventLength);
                    if (checksum != binlogChecksum)
                    {
                        throw new ExtractorException(
                                "Corrupted event in binlog (checksums do not match) at position "
                                        + startPosition
                                        + ". \nCalculated checksum : "
                                        + checksum + " - Binlog checksum : "
                                        + binlogChecksum
                                        + "\nNext event position :"
                                        + getNextEventPosition());

                    }
                }
                catch (IOException ignore)
                {
                    logger.warn("Failed to compute checksum", ignore);
                }
        }
    }

    protected BigDecimal extractDecimal(byte[] buffer, int precision, int scale)
    {
        //
        // Decimal representation in binlog seems to be as follows:
        // 1 byte - 'precision'
        // 1 byte - 'scale'
        // remaining n bytes - integer such that value = n / (10^scale)
        // Integer is represented as follows:
        // 1st bit - sign such that set == +, unset == -
        // every 4 bytes represent 9 digits in big-endian order, so that if
        // you print the values of these quads as big-endian integers one after
        // another, you get the whole number string representation in decimal.
        // What remains is to put a sign and a decimal dot.
        // 13 0a 80 00 00 05 1b 38 b0 60 00 means:
        // 0x13 - precision = 19
        // 0x0a - scale = 10
        // 0x80 - positive
        // 0x00000005 0x1b38b060 0x00
        // 5 456700000 0
        // 54567000000 / 10^{10} = 5.4567
        //
        // int_size below shows how long is integer part
        //
        // offset = offset + 2; // offset of the number part
        //
        int intg = precision - scale;
        int intg0 = intg / MysqlBinlog.DIG_PER_INT32;
        int frac0 = scale / MysqlBinlog.DIG_PER_INT32;
        int intg0x = intg - intg0 * MysqlBinlog.DIG_PER_INT32;
        int frac0x = scale - frac0 * MysqlBinlog.DIG_PER_INT32;

        int offset = 0;

        int sign = (buffer[offset] & 0x80) == 0x80 ? 1 : -1;

        // how many bytes are used to represent given amount of digits?
        int integerSize = intg0 * MysqlBinlog.SIZE_OF_INT32
                + MysqlBinlog.dig2bytes[intg0x];
        int decimalSize = frac0 * MysqlBinlog.SIZE_OF_INT32
                + MysqlBinlog.dig2bytes[frac0x];

        if (logger.isDebugEnabled())
            logger.debug("Integer size in bytes = " + integerSize
                    + " - Fraction size in bytes = " + decimalSize);
        int bin_size = integerSize + decimalSize; // total bytes
        byte[] d_copy = new byte[bin_size];

        if (bin_size > buffer.length)
        {
            throw new ArrayIndexOutOfBoundsException("Calculated bin_size: "
                    + bin_size + ", available bytes: " + buffer.length);
        }

        // Invert first bit
        d_copy[0] = buffer[0];
        d_copy[0] ^= 0x80;
        if (sign == -1)
        {
            // Invert every byte
            d_copy[0] ^= 0xFF;
        }

        for (int i = 1; i < bin_size; i++)
        {
            d_copy[i] = buffer[i];
            if (sign == -1)
            {
                // Invert every byte
                d_copy[i] ^= 0xFF;
            }
        }

        // Integer part
        offset = MysqlBinlog.dig2bytes[intg0x];

        BigDecimal intPart = new BigDecimal(0);

        if (offset > 0)
            intPart = BigDecimal.valueOf(BigEndianConversion
                    .convertNBytesToInt(d_copy, 0, offset));

        while (offset < integerSize)
        {
            intPart = intPart.movePointRight(MysqlBinlog.DIG_PER_DEC1).add(
                    BigDecimal.valueOf(BigEndianConversion.convert4BytesToInt(
                            d_copy, offset)));
            offset += 4;
        }

        // Decimal part
        BigDecimal fracPart = new BigDecimal(0);
        int shift = 0;
        for (int i = 0; i < frac0; i++)
        {
            shift += MysqlBinlog.DIG_PER_DEC1;
            fracPart = fracPart.add(BigDecimal.valueOf(
                    BigEndianConversion.convert4BytesToInt(d_copy, offset))
                    .movePointLeft(shift));
            offset += 4;
        }

        if (MysqlBinlog.dig2bytes[frac0x] > 0)
        {
            fracPart = fracPart.add(BigDecimal.valueOf(
                    BigEndianConversion.convertNBytesToInt(d_copy, offset,
                            MysqlBinlog.dig2bytes[frac0x])).movePointLeft(
                    shift + frac0x));
        }

        return BigDecimal.valueOf(sign).multiply(intPart.add(fracPart));

    }

    /**
     * Returns the number of bytes that is used to store a decimal whose
     * precision and scale are given
     * 
     * @param precision of the decimal
     * @param scale of the decimal
     * @return number of bytes used to store the decimal(precision, scale)
     */
    protected int getDecimalBinarySize(int precision, int scale)
    {
        int intg = precision - scale;
        int intg0 = intg / MysqlBinlog.DIG_PER_DEC1;
        int frac0 = scale / MysqlBinlog.DIG_PER_DEC1;
        int intg0x = intg - intg0 * MysqlBinlog.DIG_PER_DEC1;
        int frac0x = scale - frac0 * MysqlBinlog.DIG_PER_DEC1;

        assert (scale >= 0 && precision > 0 && scale <= precision);

        return intg0 * (4) + MysqlBinlog.dig2bytes[intg0x] + frac0 * (4)
                + MysqlBinlog.dig2bytes[frac0x];
    }

    public static String hexdump(byte[] buffer)
    {
        return hexdump(buffer, 0);
    }

}
