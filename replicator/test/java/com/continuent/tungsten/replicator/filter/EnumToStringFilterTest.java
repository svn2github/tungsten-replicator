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
 * Initial developer(s): Linas Virbalas
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.filter;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;

/**
 * This class tests EnumToStringFilter.
 * 
 * @author <a href="mailto:linas.virbalas@continuent.com">Linas Virbalas</a>
 */
public class EnumToStringFilterTest extends TestCase
{
    private static final String errorMsgMismatch = "Extracted enum value doesn't match one in definition";

    /**
     * Setup.
     * 
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
    }

    /**
     * Teardown.
     * 
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
    }

    /**
     * Test parsing of various enum definitions with mixed case.
     */
    public void testParseEnumMixedCase() throws Exception
    {
        String[] enumDefinition = new String[2];
        enumDefinition[0] = "enum('EMAILADDRESS','PHONENUMBER','WEBSITE','12345678901234567890123456789012345678901234567890')";
        enumDefinition[1] = "ENUM('EMAILADDRESS','PHONENUMBER','WEBSITE','12345678901234567890123456789012345678901234567890')";

        for (int i = 0; i < 2; i++)
        {
            String[] elements = EnumToStringFilter
                    .parseEnumeration(enumDefinition[i]);

            Assert.assertEquals(errorMsgMismatch, "EMAILADDRESS", elements[0]);
            Assert.assertEquals(errorMsgMismatch, "PHONENUMBER", elements[1]);
            Assert.assertEquals(errorMsgMismatch, "WEBSITE", elements[2]);
            Assert.assertEquals(errorMsgMismatch,
                    "12345678901234567890123456789012345678901234567890",
                    elements[3]);
        }
    }

    /**
     * Test parsing of enum definitions with single character.
     */
    public void testParseEnumSingleChar() throws Exception
    {
        String[] enumDefinition = new String[2];
        enumDefinition[0] = "enum('Y','N','U')";
        enumDefinition[1] = "ENUM('Y','N','U')";

        for (int i = 0; i < 2; i++)
        {
            String[] elements = EnumToStringFilter
                    .parseEnumeration(enumDefinition[i]);

            Assert.assertEquals(errorMsgMismatch, "Y", elements[0]);
            Assert.assertEquals(errorMsgMismatch, "N", elements[1]);
            Assert.assertEquals(errorMsgMismatch, "U", elements[2]);
        }
    }

    /**
     * Tests whether largest element of enumeration is determined correctly.
     */
    public void testParseEnumLargestElement() throws Exception
    {
        String largest = "12345678901234567890123456789012345678901234567890";
        String enumDefinition = "ENUM('1','12','123','1234567890','" + largest
                + "','12345')";

        String[] enumValues = EnumToStringFilter
                .parseEnumeration(enumDefinition);

        int parsedLargestPos = EnumToStringFilter.largestElement(enumValues);
        Assert.assertEquals(
                "Largest element's position determined incorrectly", 4,
                parsedLargestPos);

        int parsedLargestLen = EnumToStringFilter
                .largestElementLen(enumDefinition);

        Assert.assertEquals("Largest element's length incorrect ("
                + enumValues[parsedLargestPos] + ")", largest.length(),
                parsedLargestLen);
    }
}