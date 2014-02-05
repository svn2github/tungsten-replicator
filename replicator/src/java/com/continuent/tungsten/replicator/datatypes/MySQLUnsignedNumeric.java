/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2014 Continuent Inc.
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
 * Initial developer(s): Linas Virbalas
 * Contributor(s):  
 */

package com.continuent.tungsten.replicator.datatypes;

import java.math.BigInteger;

import org.apache.log4j.Logger;

/**
 * This class represents an UNSIGNED MySQL numeric data type.
 * 
 * @author <a href="mailto:linas.virbalas@continuent.com">Linas Virbalas</a>
 * @version 1.0
 */
public class MySQLUnsignedNumeric
{
    private static Logger          logger              = Logger.getLogger(MySQLUnsignedNumeric.class);

    public static final int        TINYINT_MAX_VALUE   = 255;
    public static final int        SMALLINT_MAX_VALUE  = 65535;
    public static final int        MEDIUMINT_MAX_VALUE = 16777215;
    public static final long       INTEGER_MAX_VALUE   = 4294967295L;
    public static final BigInteger BIGINT_MAX_VALUE    = new BigInteger(
                                                               "18446744073709551615");

    /**
     * Converts raw *negative* extracted from the binary log *unsigned* numeric
     * value into correct *positive* numeric representation. MySQL saves large
     * unsigned values as signed (negative) ones in the binary log, hence the
     * need for transformation.<br/>
     * See Issue 798 for more details.<br/>
     * Make sure your numeric value is really negative before calling this.
     * 
     * @return Converted positive number if value was negative. Same value, if
     *         it was already positive. null, if column specification was not
     *         supported.
     * @param numeric Numeric object value to convert.
     */
    public static Object negativeToMeaningful(Numeric numeric)
    {
        Object valToInsert = null;
        if (numeric.isNegative())
        {
            // Convert raw negative unsigned to positive as MySQL does.
            switch (numeric.getColumnSpec().getLength())
            {
                case 1 :
                    valToInsert = TINYINT_MAX_VALUE + 1
                            + numeric.getExtractedValue();
                    break;
                case 2 :
                    valToInsert = SMALLINT_MAX_VALUE + 1
                            + numeric.getExtractedValue();
                    break;
                case 3 :
                    valToInsert = MEDIUMINT_MAX_VALUE + 1
                            + numeric.getExtractedValue();
                    break;
                case 4 :
                    valToInsert = INTEGER_MAX_VALUE + 1
                            + numeric.getExtractedValue();
                    break;
                case 8 :
                    valToInsert = BIGINT_MAX_VALUE.add(BigInteger
                            .valueOf(1 + numeric.getExtractedValue()));
                    break;
                default :
                    logger.warn("Column length unsupported: "
                            + numeric.getColumnSpec().getLength());
                    break;
            }
            if (logger.isDebugEnabled())
                logger.debug(numeric.getExtractedValue() + " -> " + valToInsert);
        }
        else
        {
            // Positive value already - leaving as is.
            valToInsert = numeric.getExtractedValue();
        }
        return valToInsert;
    }
}
