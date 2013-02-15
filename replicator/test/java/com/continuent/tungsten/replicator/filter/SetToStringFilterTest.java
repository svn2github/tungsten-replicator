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
 * This class tests SetToStringFilter.
 * 
 * @author <a href="mailto:linas.virbalas@continuent.com">Linas Virbalas</a>
 */
public class SetToStringFilterTest extends TestCase
{
    private static final String errorMsgMismatch = "Extracted SET value doesn't match one in definition";

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
     * Test parsing of various SET definitions with mixed case.
     */
    public void testParseSetMixedCase() throws Exception
    {
        String values[] = new String[4];
        values[0] = "opt11111111111111";
        values[1] = "opt2222222222222";
        values[2] = "opt333333";
        values[3] = "OPT9012345678901234567890";

        String[] setDefinition = new String[2];
        setDefinition[0] = "set('" + values[0] + "','" + values[1] + "','"
                + values[2] + "','" + values[3] + "')";
        setDefinition[1] = "SET('" + values[0] + "','" + values[1] + "','"
                + values[2] + "','" + values[3] + "')";

        for (int i = 0; i < 2; i++)
        {
            String[] elements = SetToStringFilter.parseSet(setDefinition[i]);

            for (int v = 0; v < values.length; v++)
                Assert.assertEquals(errorMsgMismatch, values[v], elements[v]);
        }
    }

    /**
     * Test parsing of SET definitions with single character.
     */
    public void testParseSetSingleChar() throws Exception
    {
        String[] setDefinition = new String[2];
        setDefinition[0] = "set('A','B')";
        setDefinition[1] = "SET('A','B')";

        for (int i = 0; i < 2; i++)
        {
            String[] elements = SetToStringFilter.parseSet(setDefinition[i]);

            Assert.assertEquals(errorMsgMismatch, "A", elements[0]);
            Assert.assertEquals(errorMsgMismatch, "B", elements[1]);
        }
    }

    /**
     * Tests whether largest element of SET is determined correctly.
     */
    public void testParseSetLargestElement() throws Exception
    {
        String largest = "12345678901234567890123456789012345678901234567890";
        String setDefinition = "SET('1','12','123','1234567890','" + largest
                + "','12345')";

        String[] setValues = SetToStringFilter.parseSet(setDefinition);

        int parsedLargestPos = SetToStringFilter.largestElement(setValues);
        Assert.assertEquals(
                "Largest element's position determined incorrectly", 4,
                parsedLargestPos);

        int parsedLargestLen = SetToStringFilter
                .largestElementLen(setDefinition);

        Assert.assertEquals("Largest element's length incorrect ("
                + setValues[parsedLargestPos] + ")", largest.length(),
                parsedLargestLen);
    }

    /**
     * Tests whether parsing of binary encoded map is working.
     */
    public void testBinaryMapParse() throws Exception
    {
        String[] setDefs = {"opt1", "opt2", "opt3", "opt4", "opt5"};
        String[] binary = {"00000", "00001", "01000", "00011", "11001"};
        String[] expected = {"", "opt1", "opt4", "opt1,opt2", "opt1,opt4,opt5"};
        for (int i = 0; i < binary.length; i++)
        {
            Long value = Long.parseLong(binary[i], 2);
            String parsed = SetToStringFilter.binarySetMapToString(setDefs,
                    value);
            Assert.assertEquals("SET options parsed incorrectly", expected[i],
                    parsed);
        }
    }
}