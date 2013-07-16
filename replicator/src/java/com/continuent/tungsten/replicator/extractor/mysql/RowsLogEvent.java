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

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Calendar;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.database.DatabaseHelper;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnSpec;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnVal;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.extractor.ExtractorException;
import com.continuent.tungsten.replicator.extractor.mysql.conversion.BigEndianConversion;
import com.continuent.tungsten.replicator.extractor.mysql.conversion.GeneralConversion;
import com.continuent.tungsten.replicator.extractor.mysql.conversion.LittleEndianConversion;

/**
 * @author <a href="mailto:seppo.jaakola@continuent.com">Seppo Jaakola</a>
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public abstract class RowsLogEvent extends LogEvent
{
    /**
     * Fixed data part:
     * <ul>
     * <li>6 bytes. The table ID.</li>
     * <li>2 bytes. Reserved for future use.</li>
     * </ul>
     * <p>
     * Variable data part:
     * <ul>
     * <li>Packed integer. The number of columns in the table.</li>
     * <li>Variable-sized. Bit-field indicating whether each column is used, one
     * bit per column. For this field, the amount of storage required for N
     * columns is INT((N+7)/8) bytes.</li>
     * <li>Variable-sized (for UPDATE_ROWS_LOG_EVENT only). Bit-field indicating
     * whether each column is used in the UPDATE_ROWS_LOG_EVENT after-image; one
     * bit per column. For this field, the amount of storage required for N
     * columns is INT((N+7)/8) bytes.</li>
     * <li>Variable-sized. A sequence of zero or more rows. The end is
     * determined by the size of the event. Each row has the following format:
     * <ul>
     * <li>Variable-sized. Bit-field indicating whether each field in the row is
     * NULL. Only columns that are "used" according to the second field in the
     * variable data part are listed here. If the second field in the variable
     * data part has N one-bits, the amount of storage required for this field
     * is INT((N+7)/8) bytes.</li>
     * <li>Variable-sized. The row-image, containing values of all table fields.
     * This only lists table fields that are used (according to the second field
     * of the variable data part) and non-NULL (according to the previous
     * field). In other words, the number of values listed here is equal to the
     * number of zero bits in the previous field (not counting padding bits in
     * the last byte). The format of each value is described in the
     * log_event_print_value() function in log_event.cc.</li>
     * <li>(for UPDATE_ROWS_EVENT only) the previous two fields are repeated,
     * representing a second table row.</li>
     * </ul>
     * </ul>
     * Source : http://forge.mysql.com/wiki/MySQL_Internals_Binary_Log
     */
    static Logger                       logger               = Logger.getLogger(RowsLogEvent.class);

    private long                        tableId;

    protected long                      columnsNumber;

    // BITMAP
    protected BitSet                    usedColumns;

    // BITMAP for row after image
    protected BitSet                    usedColumnsForUpdate;

    /* Rows in packed format */
    protected byte[]                    packedRowsBuffer;

    /* One-after the end of the allocated space */
    protected int                       bufferSize;

    protected boolean                   useBytesForString;

    protected FormatDescriptionLogEvent descriptionEvent     = null;

    private boolean                     flagForeignKeyChecks = true;
    private boolean                     flagUniqueChecks     = true;

    public RowsLogEvent(byte[] buffer, int eventLength,
            FormatDescriptionLogEvent descriptionEvent, int eventType,
            boolean useBytesForString) throws ReplicatorException
    {
        super(buffer, descriptionEvent, eventType);
        this.descriptionEvent = descriptionEvent;

        if (logger.isDebugEnabled())
            logger.debug("Dumping rows event " + hexdump(buffer));

        this.useBytesForString = useBytesForString;

        int commonHeaderLength, postHeaderLength;

        int fixedPartIndex;

        commonHeaderLength = descriptionEvent.commonHeaderLength;
        postHeaderLength = descriptionEvent.postHeaderLength[type - 1];

        if (logger.isDebugEnabled())
            logger.debug("event length: " + eventLength
                    + " common header length: " + commonHeaderLength
                    + " post header length: " + postHeaderLength);

        try
        {
            /* Read the fixed data part */
            fixedPartIndex = commonHeaderLength;

            fixedPartIndex += MysqlBinlog.RW_MAPID_OFFSET;
            if (postHeaderLength == 6)
            {
                /*
                 * Master is of an intermediate source tree before 5.1.4. Id is
                 * 4 bytes
                 */
                tableId = LittleEndianConversion.convert4BytesToLong(buffer,
                        fixedPartIndex);
                fixedPartIndex += 4;
            }
            else
            {
                // assert (postHeaderLength ==
                // MysqlBinlog.TABLE_MAP_HEADER_LEN);
                /* 6 bytes. The table ID. */
                tableId = LittleEndianConversion.convert6BytesToLong(buffer,
                        fixedPartIndex);
                fixedPartIndex += MysqlBinlog.TM_FLAGS_OFFSET;
            }

            /*
             * Next 2 bytes are reserved for future use : no need to process
             * them for now.
             */
            readSessionVariables(buffer, fixedPartIndex);

            /* Read the variable data part of the event */
            int variableStartIndex = commonHeaderLength + postHeaderLength;

            int index = variableStartIndex;

            if (logger.isDebugEnabled())
                logger.debug("Reading number of columns from position " + index);

            long ret[] = MysqlBinlog.decodePackedInteger(buffer, index);
            columnsNumber = ret[0];
            index = (int) ret[1];

            if (logger.isDebugEnabled())
                logger.debug("Number of columns in the table = "
                        + columnsNumber);

            /*
             * Amount of storage required by bit-field indicating whether each
             * column is used for columnsNumber columns
             */
            int usedColumnsLength = (int) ((columnsNumber + 7) / 8);
            usedColumns = new BitSet(usedColumnsLength);

            if (logger.isDebugEnabled())
                logger.debug("Reading used columns bit-field from position "
                        + index);

            MysqlBinlog.setBitField(usedColumns, buffer, index,
                    (int) columnsNumber);

            index += usedColumnsLength;
            if (logger.isDebugEnabled())
                logger.debug("Bit-field of used columns "
                        + usedColumns.toString());

            if (eventType == MysqlBinlog.UPDATE_ROWS_EVENT
                    || eventType == MysqlBinlog.NEW_UPDATE_ROWS_EVENT)
            {
                usedColumnsForUpdate = new BitSet(usedColumnsLength);
                if (logger.isDebugEnabled())
                    logger.debug("Reading used columns bit-field for update from position "
                            + index);
                MysqlBinlog.setBitField(usedColumnsForUpdate, buffer, index,
                        (int) columnsNumber);
                index += usedColumnsLength;
                if (logger.isDebugEnabled())
                    logger.debug("Bit-field of used columns for update "
                            + usedColumnsForUpdate.toString());
            }

            int dataIndex = index;

            if (descriptionEvent.useChecksum())
            {
                // Removing the checksum from the size of the event
                eventLength -= 4;
            }

            int dataSize = eventLength - dataIndex;
            if (logger.isDebugEnabled())
                logger.debug("tableId: " + tableId
                        + " Number of columns in table: " + columnsNumber
                        + " Data size: " + dataSize);

            packedRowsBuffer = new byte[dataSize];
            bufferSize = dataSize;
            System.arraycopy(buffer, dataIndex, packedRowsBuffer, 0, bufferSize);

            doChecksum(buffer, eventLength, descriptionEvent);

        }
        catch (IOException e)
        {
            logger.error("Rows log event parsing failed : ", e);
        }
        return;
    }

    public abstract void processExtractedEvent(RowChangeData rowChanges,
            TableMapLogEvent map) throws ReplicatorException;

    public int getEventSize()
    {
        return packedRowsBuffer.length;
    }

    private BigDecimal extractDecimal(byte[] buffer, int precision, int scale)
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
    private int getDecimalBinarySize(int precision, int scale)
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

    protected int extractValue(ColumnSpec spec, ColumnVal value, byte[] row,
            int rowPos, int type, int meta) throws IOException,
            ReplicatorException
    {
        int length = 0;

        // Calculate length for MYSQL_TYPE_STRING
        if (type == MysqlBinlog.MYSQL_TYPE_STRING)
        {
            if (meta >= 256)
            {
                int byte0 = meta >> 8;
                int byte1 = meta & 0xFF;

                if ((byte0 & 0x30) != 0x30)
                {
                    /* a long CHAR() field: see #37426 */
                    length = byte1 | (((byte0 & 0x30) ^ 0x30) << 4);
                    type = byte0 | 0x30;
                }
                else
                {
                    switch (byte0)
                    {
                        case MysqlBinlog.MYSQL_TYPE_SET :
                        case MysqlBinlog.MYSQL_TYPE_ENUM :
                        case MysqlBinlog.MYSQL_TYPE_STRING :
                            type = byte0;
                            length = byte1;
                            break;

                        default :
                        {
                            logger.error("Don't know how to handle column type");
                            return 0;
                        }
                    }
                }
            }
            else
            {
                length = meta;
            }
        }

        if (logger.isDebugEnabled())
            logger.debug("Handling type " + type + " - meta = " + meta);
        switch (type)
        {
            case MysqlBinlog.MYSQL_TYPE_LONG :
            {
                int si = (int) LittleEndianConversion
                        .convertSignedNBytesToLong(row, rowPos, 4);

                if (si < MysqlBinlog.INT_MIN || si > MysqlBinlog.INT_MAX)
                {
                    logger.error("int out of range: " + si + "(range: "
                            + MysqlBinlog.INT_MIN + " - " + MysqlBinlog.INT_MAX
                            + " )");
                }
                value.setValue(new Integer(si));
                if (spec != null)
                {
                    spec.setType(java.sql.Types.INTEGER);
                    spec.setLength(4);
                }
                return 4;
            }

            case MysqlBinlog.MYSQL_TYPE_TINY :
            {
                short si = BigEndianConversion.convert1ByteToShort(row, rowPos);
                if (si < MysqlBinlog.TINYINT_MIN
                        || si > MysqlBinlog.TINYINT_MAX)
                {
                    logger.error("tinyint out of range: " + si + "(range: "
                            + MysqlBinlog.TINYINT_MIN + " - "
                            + MysqlBinlog.TINYINT_MAX + " )");
                }

                value.setValue(new Integer(si));
                if (spec != null)
                {
                    spec.setType(java.sql.Types.INTEGER);
                    spec.setLength(1);
                }
                return 1;
            }

            case MysqlBinlog.MYSQL_TYPE_SHORT :
            {
                short si = (short) LittleEndianConversion
                        .convertSignedNBytesToLong(row, rowPos, 2);
                if (si < MysqlBinlog.SMALLINT_MIN
                        || si > MysqlBinlog.SMALLINT_MAX)
                {
                    logger.error("smallint out of range: " + si + "(range: "
                            + MysqlBinlog.SMALLINT_MIN + " - "
                            + MysqlBinlog.SMALLINT_MAX + " )");
                }
                value.setValue(new Integer(si));
                if (spec != null)
                {
                    spec.setType(java.sql.Types.INTEGER);
                    spec.setLength(2);
                }
                return 2;
            }

            case MysqlBinlog.MYSQL_TYPE_INT24 :
            {
                int si = (int) LittleEndianConversion
                        .convertSignedNBytesToLong(row, rowPos, 3);
                if (si < MysqlBinlog.MEDIUMINT_MIN
                        || si > MysqlBinlog.MEDIUMINT_MAX)
                {
                    logger.error("mediumint out of range: " + si + "(range: "
                            + MysqlBinlog.MEDIUMINT_MIN + " - "
                            + MysqlBinlog.MEDIUMINT_MAX + " )");
                }
                value.setValue(new Integer(si));
                if (spec != null)
                {
                    spec.setType(java.sql.Types.INTEGER);
                    spec.setLength(3);
                }
                return 3;
            }

            case MysqlBinlog.MYSQL_TYPE_LONGLONG :
            {
                long si = LittleEndianConversion.convertSignedNBytesToLong(row,
                        rowPos, 8);
                if (si < 0)
                {
                    long ui = LittleEndianConversion.convert8BytesToLong(row,
                            rowPos);
                    value.setValue(new Long(ui));
                    if (spec != null)
                    {
                        spec.setType(java.sql.Types.INTEGER);
                        spec.setLength(8);
                    }
                }
                else
                {
                    value.setValue(new Long(si));
                    if (spec != null)
                    {
                        spec.setType(java.sql.Types.INTEGER);
                        spec.setLength(8);
                    }
                }
                return 8;
            }

            case MysqlBinlog.MYSQL_TYPE_NEWDECIMAL :
            {
                int precision = meta >> 8;
                int decimals = meta & 0xFF;
                int bin_size = getDecimalBinarySize(precision, decimals);
                byte[] dec = new byte[bin_size];
                for (int i = 0; i < bin_size; i++)
                    dec[i] = row[rowPos + i];
                BigDecimal myDouble = extractDecimal(dec, precision, decimals);
                value.setValue(myDouble);
                if (spec != null)
                    spec.setType(java.sql.Types.DECIMAL);
                return bin_size;
            }

            case MysqlBinlog.MYSQL_TYPE_FLOAT :
            {
                float fl = MysqlBinlog.float4ToFloat(row, rowPos);
                value.setValue(new Float(fl));
                if (spec != null)
                    spec.setType(java.sql.Types.FLOAT);
                return 4;
            }

            case MysqlBinlog.MYSQL_TYPE_DOUBLE :
            {
                double dbl = MysqlBinlog.double8ToDouble(row, rowPos);
                value.setValue(new Double(dbl));
                if (spec != null)
                    spec.setType(java.sql.Types.DOUBLE);

                return 8;
            }

            case MysqlBinlog.MYSQL_TYPE_BIT :
            {
                /* Meta-data: bit_len, bytes_in_rec, 2 bytes */
                int nbits = ((meta >> 8) * 8) + (meta & 0xFF);
                length = (nbits + 7) / 8;

                /*
                 * This code has come from observations of patterns in the MySQL
                 * binlog. It is not directly from reading any public domain
                 * C-source code. The test cases included a variety of bit(x)
                 * columns from 1 bit up to 28 bits. This length appears to be
                 * correctly calculated and the bit values themselves are in a
                 * simple, non byte swapped byte array.
                 */

                int retval = (int) MysqlBinlog.ulNoSwapToInt(row, rowPos,
                        length);
                value.setValue(new Integer(retval));
                if (spec != null)
                    spec.setType(java.sql.Types.BIT);
                return length;
            }

            case MysqlBinlog.MYSQL_TYPE_TIMESTAMP :
            {
                long i32 = LittleEndianConversion.convertNBytesToLong_2(row,
                        rowPos, 4);

                if (i32 == 0)
                {
                    value.setValue(Integer.valueOf(0));
                }
                else
                    // convert sec based timestamp to millisecond precision
                    value.setValue(new java.sql.Timestamp(i32 * 1000));
                if (spec != null)
                    spec.setType(java.sql.Types.TIMESTAMP);
                return 4;
            }
            case MysqlBinlog.MYSQL_TYPE_TIMESTAMP2 :
            {
                int secPartsLength = 0;
                long i32 = BigEndianConversion.convertNBytesToLong(row, rowPos,
                        4);

                if (logger.isDebugEnabled())
                {
                    logger.debug("Extracting timestamp "
                            + hexdump(row, rowPos, 4));
                    logger.debug("Meta value is " + meta);
                    logger.debug("Value as integer is " + i32);
                }
                if (i32 == 0)
                {
                    value.setValue(Integer.valueOf(0));
                }
                else
                {
                    // convert sec based timestamp to millisecond precision
                    Timestamp tsVal = new java.sql.Timestamp(i32 * 1000);
                    if (logger.isDebugEnabled())
                        logger.debug("Setting value to " + tsVal);

                    value.setValue(tsVal);
                    secPartsLength = getSecondPartsLength(meta);
                    rowPos += 4;
                    tsVal.setNanos(extractNanoseconds(row, rowPos, meta,
                            secPartsLength));
                }

                if (spec != null)
                    spec.setType(java.sql.Types.TIMESTAMP);
                return 4 + secPartsLength;
            }
            case MysqlBinlog.MYSQL_TYPE_DATETIME :
            {
                long i64 = LittleEndianConversion.convert8BytesToLong(row,
                        rowPos); /* YYYYMMDDhhmmss */

                // Let's check for zero date
                if (i64 == 0)
                {
                    value.setValue(Integer.valueOf(0));
                    if (spec != null)
                        spec.setType(java.sql.Types.TIMESTAMP);
                    return 8;
                }

                // calculate year, month...sec components of timestamp
                long d = i64 / 1000000;
                int year = (int) (d / 10000);
                int month = (int) (d % 10000) / 100;
                int day = (int) (d % 10000) % 100;

                long t = i64 % 1000000;
                int hour = (int) (t / 10000);
                int min = (int) (t % 10000) / 100;
                int sec = (int) (t % 10000) % 100;

                // construct timestamp from time components
                java.sql.Timestamp ts = null;

                Calendar cal = Calendar.getInstance();
                // Month value is 0-based. e.g., 0 for January.
                cal.set(year, month - 1, day, hour, min, sec);

                ts = new Timestamp(cal.getTimeInMillis());
                // Clear the nanos (no data)
                ts.setNanos(0);

                value.setValue(ts);
                if (spec != null)
                    spec.setType(java.sql.Types.TIMESTAMP);
                return 8;
            }
            case MysqlBinlog.MYSQL_TYPE_DATETIME2 :
            {
                /**
                 * 1 bit sign (used when on disk)<br>
                 * 17 bits year*13+month (year 0-9999, month 0-12)<br>
                 * 5 bits day (0-31)<br>
                 * 5 bits hour (0-23)<br>
                 * 6 bits minute (0-59)<br>
                 * 6 bits second (0-59)<br>
                 * 24 bits microseconds (0-999999)<br>
                 * Total: 64 bits = 8 bytes SYYYYYYY.YYYYYYYY.YYdddddh
                 * .hhhhmmmm.mmssssss.ffffffff.ffffffff.ffffffff
                 */
                long i64 = BigEndianConversion.convertNBytesToLong(row, rowPos,
                        5) - 0x8000000000L;

                // Let's check for zero date
                if (i64 == 0)
                {
                    value.setValue(Integer.valueOf(0));
                    if (spec != null)
                        spec.setType(java.sql.Types.TIMESTAMP);
                    return 8;
                }

                long currentValue = (i64 >> 22);
                int year = (int) (currentValue / 13);
                int month = (int) (currentValue % 13);

                long previousValue = currentValue;
                currentValue = i64 >> 17;
                int day = (int) (currentValue - (previousValue << 5));

                previousValue = currentValue;
                currentValue = (i64 >> 12);

                int hour = (int) (currentValue - (previousValue << 5));

                previousValue = currentValue;
                currentValue = (i64 >> 6);

                int minute = (int) (currentValue - (previousValue << 6));

                previousValue = currentValue;
                currentValue = i64;

                int seconds = (int) (currentValue - (previousValue << 6));
                if (logger.isDebugEnabled())
                    logger.debug("Time " + hour + ":" + minute + ":" + seconds);

                // construct timestamp from time components
                java.sql.Timestamp ts = null;

                Calendar cal = Calendar.getInstance();
                if (logger.isDebugEnabled())
                    logger.debug("Timezone is "
                            + cal.getTimeZone().getDisplayName());

                // Month value is 0-based. e.g., 0 for January.
                cal.set(year, month - 1, day, hour, minute, seconds);

                ts = new Timestamp(cal.getTimeInMillis());

                value.setValue(ts);
                if (spec != null)
                    spec.setType(java.sql.Types.TIMESTAMP);

                int secPartsLength = getSecondPartsLength(meta);
                rowPos += 5;
                ts.setNanos(extractNanoseconds(row, rowPos, meta,
                        secPartsLength));

                return 5 + secPartsLength;
            }
            case MysqlBinlog.MYSQL_TYPE_TIME :
            {
                long i32 = LittleEndianConversion.convert3BytesToInt(row,
                        rowPos);
                value.setValue(java.sql.Time.valueOf(i32 / 10000 + ":"
                        + (i32 % 10000) / 100 + ":" + i32 % 100));
                if (spec != null)
                    spec.setType(java.sql.Types.TIME);
                return 3;
            }

            case MysqlBinlog.MYSQL_TYPE_TIME2 :
            {
                /**
                 * 1 bit sign (Used for sign, when on disk)<br>
                 * 1 bit unused (Reserved for wider hour range, e.g. for
                 * intervals)<br>
                 * 10 bit hour (0-836)<br>
                 * 6 bit minute (0-59)<br>
                 * 6 bit second (0-59)<br>
                 * 24 bits microseconds (0-999999)<br>
                 * Total: 48 bits = 6 bytes
                 * Suhhhhhh.hhhhmmmm.mmssssss.ffffffff.ffffffff.ffffffff
                 */
                if (logger.isDebugEnabled())
                    logger.debug("Extracting TIME2 from position " + rowPos
                            + " : " + hexdump(row, rowPos, 3));
                long i32 = (BigEndianConversion.convert3BytesToInt(row, rowPos) - 0x800000L) & 0xBFFFFFL;

                long currentValue = (i32 >> 12);
                int hours = (int) currentValue;

                long previousValue = currentValue;
                currentValue = i32 >> 6;
                int minutes = (int) (currentValue - (previousValue << 6));

                previousValue = currentValue;
                currentValue = i32;
                int seconds = (int) (currentValue - (previousValue << 6));

                Time time = java.sql.Time.valueOf(hours + ":" + minutes + ":"
                        + seconds);

                Timestamp tsVal = new java.sql.Timestamp(time.getTime());
                value.setValue(tsVal);

                int secPartsLength = getSecondPartsLength(meta);
                rowPos += 3;
                int nanoseconds = extractNanoseconds(row, rowPos, meta,
                        secPartsLength);
                tsVal.setNanos(nanoseconds);

                if (spec != null)
                    spec.setType(java.sql.Types.TIME);
                return 3 + secPartsLength;
            }

            case MysqlBinlog.MYSQL_TYPE_DATE :
            {
                int i32 = 0;
                i32 = LittleEndianConversion.convert3BytesToInt(row, rowPos);
                java.sql.Date date = null;

                // Let's check if the date is 0000-00-00
                if (i32 == 0)
                {
                    value.setValue(Integer.valueOf(0));
                    if (spec != null)
                        spec.setType(java.sql.Types.DATE);
                    return 3;
                }

                Calendar cal = Calendar.getInstance();
                cal.clear();
                // Month value is 0-based. e.g., 0 for January.
                cal.set(i32 / (16 * 32), (i32 / 32 % 16) - 1, i32 % 32);

                date = new Date(cal.getTimeInMillis());

                value.setValue(date);
                if (spec != null)
                    spec.setType(java.sql.Types.DATE);
                return 3;
            }

            case MysqlBinlog.MYSQL_TYPE_YEAR :
            {
                int i32 = LittleEndianConversion.convert1ByteToInt(row, rowPos);
                // raw value is offset by 1900. e.g. "1" is 1901.
                value.setValue(1900 + i32);
                // It might seem more correct to create a java.sql.Types.DATE
                // value for this date, but it is much simpler to pass the value
                // as an integer. The MySQL JDBC specification states that one
                // can pass a java int between 1901 and 2055. Creating a DATE
                // value causes truncation errors with certain SQL_MODES
                // (e.g."STRICT_TRANS_TABLES").
                if (spec != null)
                    spec.setType(java.sql.Types.INTEGER);
                return 1;
            }

            case MysqlBinlog.MYSQL_TYPE_ENUM :
                switch (length)
                {
                    case 1 :
                    {
                        int i32 = LittleEndianConversion.convert1ByteToInt(row,
                                rowPos);
                        value.setValue(new Integer(i32));
                        if (spec != null)
                            spec.setType(java.sql.Types.OTHER);
                        return 1;
                    }
                    case 2 :
                    {
                        int i32 = LittleEndianConversion.convert2BytesToInt(
                                row, rowPos);
                        value.setValue(new Integer(i32));
                        if (spec != null)
                            spec.setType(java.sql.Types.INTEGER);
                        return 2;
                    }
                    default :
                        return 0;
                }

            case MysqlBinlog.MYSQL_TYPE_SET :
                long val = LittleEndianConversion.convertNBytesToLong_2(row,
                        rowPos, length);
                value.setValue(new Long(val));
                if (spec != null)
                    spec.setType(java.sql.Types.INTEGER);
                return length;

            case MysqlBinlog.MYSQL_TYPE_BLOB :
                /*
                 * BLOB or TEXT datatype
                 */
                if (spec != null)
                    spec.setType(java.sql.Types.BLOB);
                int blob_size = 0;
                switch (meta)
                {
                    case 1 :
                        length = GeneralConversion
                                .unsignedByteToInt(row[rowPos]);
                        blob_size = 1;
                        break;
                    case 2 :
                        length = LittleEndianConversion.convert2BytesToInt(row,
                                rowPos);
                        blob_size = 2;
                        break;
                    case 3 :
                        length = LittleEndianConversion.convert3BytesToInt(row,
                                rowPos);
                        blob_size = 3;
                        break;
                    case 4 :
                        length = (int) LittleEndianConversion
                                .convert4BytesToLong(row, rowPos);
                        blob_size = 4;
                        break;
                    default :
                        logger.error("Unknown BLOB packlen= " + length);
                        return 0;
                }
                try
                {
                    SerialBlob blob = DatabaseHelper.getSafeBlob(row, rowPos
                            + blob_size, length);
                    value.setValue(blob);
                }
                catch (SQLException e)
                {
                    throw new MySQLExtractException(
                            "Failure while extracting blob", e);
                }
                if (spec != null)
                {
                    spec.setType(java.sql.Types.BLOB);
                }
                return length + blob_size;

            case MysqlBinlog.MYSQL_TYPE_VARCHAR :
            case MysqlBinlog.MYSQL_TYPE_VAR_STRING :
                /*
                 * Except for the data length calculation, MYSQL_TYPE_VARCHAR,
                 * MYSQL_TYPE_VAR_STRING and MYSQL_TYPE_STRING are handled the
                 * same way
                 */
                length = meta;
                if (length < 256)
                {
                    length = LittleEndianConversion.convert1ByteToInt(row,
                            rowPos);
                    rowPos++;
                    if (useBytesForString)
                        value.setValue(processStringAsBytes(row, rowPos, length));
                    else
                        value.setValue(processString(row, rowPos, length));
                    length += 1;
                }
                else
                {
                    length = LittleEndianConversion.convert2BytesToInt(row,
                            rowPos);
                    rowPos += 2;
                    if (useBytesForString)
                        value.setValue(processStringAsBytes(row, rowPos, length));
                    else
                        value.setValue(processString(row, rowPos, length));
                    length += 2;
                }

                if (spec != null)
                    spec.setType(java.sql.Types.VARCHAR);
                return length;

            case MysqlBinlog.MYSQL_TYPE_STRING :
                if (length < 256)
                {
                    length = LittleEndianConversion.convert1ByteToInt(row,
                            rowPos);
                    rowPos++;
                    if (useBytesForString)
                        value.setValue(processStringAsBytes(row, rowPos, length));
                    else
                        value.setValue(processString(row, rowPos, length));
                    length += 1;
                }
                else
                {
                    length = LittleEndianConversion.convert2BytesToInt(row,
                            rowPos);
                    rowPos += 2;
                    if (useBytesForString)
                        value.setValue(processStringAsBytes(row, rowPos, length));
                    else
                        value.setValue(processString(row, rowPos, length));
                    length += 2;
                }
                if (spec != null)
                    spec.setType(java.sql.Types.VARCHAR);
                return length;

            default :
            {
                throw new MySQLExtractException("unknown data type " + type);
            }
        }
    }

    private int extractNanoseconds(byte[] row, int rowPos, int meta,
            int secPartsLength)
    {
        if (meta > 0)
        {
            // Extract second parts
            int readValue = BigEndianConversion.convertNBytesToInt(row, rowPos,
                    secPartsLength);

            int i = readValue * 1000;
            switch (meta)
            {
                case 1 :
                case 2 :
                    i *= 10000;
                    break;
                case 3 :
                case 4 :
                    i *= 100;
                    break;
                case 5 :
                case 6 :
                    break;
                default :
                    break;
            }

            return i;
        }
        return 0;
    }

    private int getSecondPartsLength(int meta)
    {
        return (meta + 1) / 2;
    }

    // JIRA TREP-237. Need to expose the table ID.
    protected long getTableId()
    {
        return tableId;
    }

    private byte[] processStringAsBytes(byte[] buffer, int pos, int length)
            throws ReplicatorException
    {
        byte[] output = new byte[length];
        System.arraycopy(buffer, pos, output, 0, length);
        return output;
    }

    protected String processString(byte[] buffer, int pos, int length)
            throws ReplicatorException
    {
        return new String(buffer, pos, length);
    }

    protected int processExtractedEventRow(OneRowChange oneRowChange,
            int rowIndex, BitSet cols, int rowPos, byte[] row,
            TableMapLogEvent map, boolean isKeySpec) throws ReplicatorException
    {
        int startIndex = rowPos;
        if (logger.isDebugEnabled())
        {
            logger.debug("processExtractedEventRow " + hexdump(row)
                    + " from position " + startIndex);
            logger.debug(oneRowChange.getAction().toString() + " for table "
                    + oneRowChange.getSchemaName() + "."
                    + oneRowChange.getTableName());
        }
        int usedColumnsCount = 0;
        for (int i = 0; i < columnsNumber; i++)
        {
            if (cols.get(i))
                usedColumnsCount++;
        }
        BitSet nulls = new BitSet(usedColumnsCount);
        MysqlBinlog.setBitField(nulls, row, startIndex, usedColumnsCount);

        ArrayList<ArrayList<OneRowChange.ColumnVal>> rows = (isKeySpec)
                ? oneRowChange.getKeyValues()
                : oneRowChange.getColumnValues();

        /*
         * add new row for column values
         */
        if (rows.size() == rowIndex)
        {
            rows.add(new ArrayList<ColumnVal>());
        }
        ArrayList<OneRowChange.ColumnVal> columns = rows.get(rowIndex);

        if (columns == null)
        {
            throw new ExtractorException(
                    "Row data corrupted : column value list empty for row "
                            + oneRowChange.toString());
        }
        rowPos += (usedColumnsCount + 7) / 8;

        OneRowChange.ColumnSpec spec = null;
        int nullIndex = 0;

        for (int i = 0; i < map.getColumnsCount(); i++)
        {
            if (logger.isDebugEnabled())
                logger.debug("Extracting column " + (i + 1) + " out of "
                        + map.getColumnsCount());

            if (cols.get(i) == false)
                continue;

            boolean isNull = nulls.get(nullIndex);
            nullIndex++;

            OneRowChange.ColumnVal value = oneRowChange.new ColumnVal();
            if (isKeySpec)
            {
                if (rowIndex == 0)
                {
                    spec = oneRowChange.new ColumnSpec();
                    spec.setIndex(i + 1);
                    oneRowChange.getKeySpec().add(spec);
                }

                oneRowChange.getKeyValues().get(rowIndex).add(value);
            }
            else
            {
                if (rowIndex == 0)
                {
                    spec = oneRowChange.new ColumnSpec();
                    spec.setIndex(i + 1);
                    oneRowChange.getColumnSpec().add(spec);
                }
                else
                {
                    // Check if column was null until now
                    ColumnSpec columnSpec = oneRowChange.getColumnSpec().get(i);
                    if (columnSpec != null
                            && columnSpec.getType() == java.sql.Types.NULL
                            && !isNull)
                    {
                        spec = columnSpec;
                    }
                    else
                        spec = null;
                }
                oneRowChange.getColumnValues().get(rowIndex).add(value);
            }
            if (isNull)
            {
                value.setValueNull();
            }
            else
            {
                int size = 0;
                try
                {
                    size = extractValue(
                            spec,
                            value,
                            row,
                            rowPos,
                            LittleEndianConversion.convert1ByteToInt(
                                    map.getColumnsTypes(), i),
                            map.getMetadata()[i]);
                }
                catch (IOException e)
                {
                    throw new ExtractorException(
                            "Row column value parsing failure", e);
                }
                if (size == 0)
                {
                    return 0;
                }
                rowPos += size;
            }
        }

        return rowPos - startIndex;
    }

    private void readSessionVariables(byte[] buffer, int pos)
            throws IOException
    {
        String sessionVariables;
        int flags;

        final int OPTION_NO_FOREIGN_KEY_CHECKS = 1 << 1;
        final int OPTION_RELAXED_UNIQUE_CHECKS = 1 << 2;

        flags = LittleEndianConversion.convert2BytesToInt(buffer, pos);

        flagForeignKeyChecks = (flags & OPTION_NO_FOREIGN_KEY_CHECKS) != OPTION_NO_FOREIGN_KEY_CHECKS;
        flagUniqueChecks = (flags & OPTION_RELAXED_UNIQUE_CHECKS) != OPTION_RELAXED_UNIQUE_CHECKS;

        sessionVariables = "set @@session.foreign_key_checks="
                + (flagForeignKeyChecks ? 1 : 0) + ", @@session.unique_checks="
                + (flagUniqueChecks ? 1 : 0);

        if (logger.isDebugEnabled())
        {
            logger.debug(sessionVariables);
        }
    }

    /**
     * Returns the flagForeignKeyChecks value.
     * 
     * @return Returns the flagForeignKeyChecks.
     */
    public String getForeignKeyChecksFlag()
    {
        return (flagForeignKeyChecks ? "1" : "0");
    }

    /**
     * Returns the flagUniqueChecks value.
     * 
     * @return Returns the flagUniqueChecks.
     */
    public String getUniqueChecksFlag()
    {
        return (flagUniqueChecks ? "1" : "0");
    }
}
