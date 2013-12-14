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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.continuent.tungsten.common.file.FilePath;

/**
 * Implements unit test for file IO operations designed to be invoked by
 * different implementations.
 */
public class FileIOTest
{
    // Must be provided by subclasses.
    protected FileIO fileIO;

    /**
     * Setup.
     * 
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        fileIO = new FileIO();
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
     * Verify that every unit test has an empty test directory that exists, is
     * readable, and is writable.
     */
    @Test
    public void testTestDir() throws Exception
    {
        FilePath testDir = prepareTestDir("testTestDir");
        Assert.assertTrue("Exists: " + testDir, fileIO.exists(testDir));
        Assert.assertTrue("Is directory: " + testDir,
                fileIO.isDirectory(testDir));
        Assert.assertFalse("Is file: " + testDir, fileIO.isFile(testDir));
        Assert.assertTrue("Is writable: " + testDir, fileIO.writable(testDir));
        Assert.assertTrue("Is readable: " + testDir, fileIO.readable(testDir));
        Assert.assertEquals("Has no children: " + testDir, 0,
                fileIO.list(testDir).length);
    }

    /**
     * Verify that we can write a file and read it back.
     * 
     * @throws Exception
     */
    @Test
    public void testWriteRead() throws Exception
    {
        FilePath testDir = prepareTestDir("testWriteRead");

        // Write to the file and ensure it exists thereafter.
        FilePath fp = new FilePath(testDir, "foo");
        Assert.assertFalse("File does not exist: " + fp, fileIO.exists(fp));
        fileIO.write(fp, "bar");
        Assert.assertTrue("File exists: " + fp, fileIO.exists(fp));
        Assert.assertTrue("Is a file: " + fp, fileIO.isFile(fp));
        Assert.assertFalse("Is a directory: " + fp, fileIO.isDirectory(fp));

        // Read the value back and ensure it matches.
        String contents = fileIO.read(fp);
        Assert.assertEquals("File contents should match what we wrote", "bar",
                contents);
    }

    /**
     * Verify that we can can create children of a directory, correctly list
     * them, and then delete them.
     */
    @Test
    public void testDirectoryChildren() throws Exception
    {
        FilePath testDir = prepareTestDir("testDirectoryChildren");

        // Create a directory.
        FilePath dir1 = new FilePath(testDir, "dir1");
        boolean createdDir1 = fileIO.mkdir(dir1);
        Assert.assertTrue("Created dir: " + dir1, createdDir1);
        Assert.assertEquals("Has no children: " + dir1, 0,
                fileIO.list(dir1).length);

        // Add another directory and a file.
        FilePath dir1File1 = new FilePath(dir1, "file1");
        fileIO.write(dir1File1, "file1 contents");

        FilePath dir1Dir2 = new FilePath(dir1, "dir2");
        fileIO.mkdirs(dir1Dir2);

        // Ensure we have expected counts of children.
        int dir1Children = fileIO.list(dir1).length;
        Assert.assertEquals("dir 1 has children", 2, dir1Children);

        int dir1File1Children = fileIO.list(dir1File1).length;
        Assert.assertEquals("dir 1 file1 has no children", 0, dir1File1Children);

        int dir1Dir2Children = fileIO.list(dir1Dir2).length;
        Assert.assertEquals("dir 1 dir2 has no children", 0, dir1Dir2Children);

        // Ensure that we cannot delete dir1 if the delete is not recursive.
        boolean deleted1 = fileIO.delete(dir1, false);
        Assert.assertFalse("Unable to delete: " + dir1, deleted1);
        Assert.assertTrue("Exists: " + dir1, fileIO.exists(dir1));

        // Ensure that we delete everything if the dir1 delete is recursive.
        boolean deleted2 = fileIO.delete(dir1, true);
        Assert.assertTrue("Unable to delete: " + dir1, deleted2);
        Assert.assertFalse("Does not exists: " + dir1, fileIO.exists(dir1));
    }

    // Sets up a test directory.
    private FilePath prepareTestDir(String dirName) throws Exception
    {
        FilePath testDir = new FilePath(dirName);
        fileIO.delete(testDir, true);
        if (fileIO.exists(testDir))
            throw new Exception("Unable to clear test directory: " + dirName);
        fileIO.mkdirs(testDir);
        if (!fileIO.exists(testDir))
            throw new Exception("Unable to create test directory: " + dirName);
        return testDir;
    }
}