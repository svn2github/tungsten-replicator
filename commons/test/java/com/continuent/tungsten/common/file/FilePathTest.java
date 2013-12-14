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

package com.continuent.tungsten.common.file;

import junit.framework.Assert;

import org.junit.Test;

import com.continuent.tungsten.common.file.FilePath;

/**
 * Implements a unit test for file path operations.
 */
public class FilePathTest
{
    /**
     * Verify that we can create a basic path.
     */
    @Test
    public void testPathCreation()
    {
        FilePath pb = new FilePath("/foo/bar");
        Assert.assertTrue("Absolute path", pb.isAbsolute());
        Assert.assertEquals("Checking for correct # of elements", 2, pb
                .elements().size());
        Assert.assertEquals("Path element #0", "foo", pb.element(0));
        Assert.assertEquals("Path element #1", "bar", pb.element(1));
    }

    /**
     * Verify that paths containing legal forms are properly normalized into a
     * single path with no trailing slash, missing elements, or empty elements.
     */
    @Test
    public void testNormalization()
    {
        String[] in = {"/", "/foo/", "/foo/../bar", "/foo/./bar"};
        String[] normalizedOut = {"/", "/foo", "/bar", "/foo/bar"};
        for (int i = 0; i < in.length; i++)
        {
            FilePath pb = new FilePath(in[i]);
            String out = pb.toString();
            Assert.assertEquals("Checking output of " + in[i],
                    normalizedOut[i], out);
        }
    }

    /**
     * Verify that append() methods correct append elements to the path. This
     * checks a variety of sub-cases.
     */
    @Test
    public void testAppend()
    {
        // Start with nothing.
        FilePath pb = new FilePath();
        Assert.assertEquals("Checking path state", "/", pb.toString());

        // Append a simple path to empty path.
        pb.append("foobar");
        Assert.assertEquals("Checking path state", "/foobar", pb.toString());

        // Append an element to a non-empty path.
        pb.append("more");
        Assert.assertEquals("Checking path state", "/foobar/more",
                pb.toString());

        // Append a .., which chops off a single element.
        pb.append("..");
        Assert.assertEquals("Checking path state", "/foobar", pb.toString());

        // Append a ., which has no effect.
        pb.append(".");
        Assert.assertEquals("Checking path state", "/foobar", pb.toString());

        // Append another .., which chops off back to the root.
        pb.append("..");
        Assert.assertEquals("Checking path state", "/", pb.toString());

        // Append a full path to the now empty test path.
        FilePath pb2 = new FilePath("/my/new/path");
        pb.append(pb2);
        Assert.assertEquals("Checking path state", "/my/new/path",
                pb.toString());

        // Append an empty path to the test path. This should make no
        // difference.
        pb.append(new FilePath("/"));
        Assert.assertEquals("Checking path state", "/my/new/path",
                pb.toString());

        // Append path elements up to the top, which should truncate to a new
        // absolute path.
        pb.append(new FilePath("../../../different/path"));
        Assert.assertEquals("Checking path state", "/different/path",
                pb.toString());
    }
}