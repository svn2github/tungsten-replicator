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

package com.continuent.tungsten.replicator.csv;

import java.util.TimeZone;

import com.continuent.tungsten.replicator.ReplicatorException;

/**
 * Denotes a class that formats object for writing to CSV. Instances of this
 * class allow users to choose a preferred format for representing string
 * values, which can vary independently of the conventions for CSV formatting
 * such as line and column separator characters.
 */
public interface CsvDataFormat
{
    /** Time zone to use for date/time conversions */
    public void setTimeZone(TimeZone tz);

    /** Ready the converter for use. */
    public void prepare();

    /** Converts value to a CSV-ready string. */
    public String csvString(Object value, int javaType, boolean blob)
            throws ReplicatorException;
}