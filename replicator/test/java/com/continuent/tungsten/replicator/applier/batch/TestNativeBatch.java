/**
 * Tungsten: An Application Server for uni/cluster.
 * Copyright (C) 2012 Continuent Inc.
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

package com.continuent.tungsten.replicator.applier.batch;

import java.io.StringReader;
import java.util.HashMap;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

/**
 * Tests ability to handle parameterized SQL scripts using SqlScriptGenerator
 * class.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class TestNativeBatch
{
    /**
     * Test adding and recognizing comments on basic DDL statements.
     */
    @Test
    public void testSimpleScriptGeneration() throws Exception
    {
        // Input data.
        StringBuffer sb = new StringBuffer();
        sb.append("line 1 %%param1%%\n");
        sb.append("  line1 cont %%param2%%");

        // Load raw commands.
        BatchScript gen = new BatchScript();
        StringReader sr = new StringReader(sb.toString());
        gen.load(null, sr);

        // Apply parameters and check.
        HashMap<String, String> params = new HashMap<String, String>();
        params.put("%%param1%%", "X");
        params.put("%%param2%%", "Y");
        List<BatchCommand> commands = gen.getParameterizedScript(params);

        Assert.assertEquals("1 command returned", 1, commands.size());
        Assert.assertEquals("First line", 1, commands.get(0).getLineNumber());
        Assert.assertTrue("Should have param 1", commands.get(0).getCommand()
                .indexOf("X") > -1);
        Assert.assertTrue("Should have param 2", commands.get(0).getCommand()
                .indexOf("Y") > -1);

        // Ensure toString() works.
        Assert.assertNotNull("To string works", commands.get(0).toString());
    }

    /**
     * Test adding and recognizing comments on basic DDL statements.
     */
    @Test
    public void testSimpleScriptGeneration2() throws Exception
    {
        // Input data.
        StringBuffer sb = new StringBuffer();
        sb.append("line 1 %%param1%%\n");
        sb.append("  line1 cont\n");
        sb.append("line 2 %%param3%%\n");

        // Load raw commands.
        BatchScript gen = new BatchScript();
        StringReader sr = new StringReader(sb.toString());
        gen.load(null, sr);

        // Apply parameters and check.
        HashMap<String, String> params = new HashMap<String, String>();
        params.put("%%param1%%", "X");
        params.put("%%param2%%", "Y");
        params.put("%%param3%%", "Z");
        List<BatchCommand> commands = gen.getParameterizedScript(params);

        Assert.assertEquals("2 commands returned", 2, commands.size());
        Assert.assertTrue("Should have param 1", commands.get(0).getCommand()
                .indexOf("X") > -1);
        Assert.assertFalse("Should not have param 2", commands.get(0)
                .getCommand().indexOf("Y") > -1);

        Assert.assertEquals("Third line", 3, commands.get(1).getLineNumber());
        Assert.assertFalse("Should not have param 2", commands.get(1)
                .getCommand().indexOf("Y") > -1);
        Assert.assertTrue("Should have param 3", commands.get(1).getCommand()
                .indexOf("Z") > -1);
    }

    /**
     * Confirm that whitespace and comments are not interpreted as commands.
     */
    @Test
    public void testEmptyBatch() throws Exception
    {
        // Input data.
        StringBuffer sb = new StringBuffer();
        sb.append("  ");
        sb.append("# a comment");
        sb.append("#");
        sb.append(" \t ");
        sb.append("");

        // Load raw commands.
        BatchScript gen = new BatchScript();
        StringReader sr = new StringReader(sb.toString());
        gen.load(null, sr);

        // Get the commands.
        HashMap<String, String> params = new HashMap<String, String>();
        List<BatchCommand> commands = gen.getParameterizedScript(params);

        Assert.assertEquals("0 commands found", 0, commands.size());
    }
}