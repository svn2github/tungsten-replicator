/**
 * Tungsten Scale-Out Stack
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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.continuent.tungsten.replicator.ReplicatorException;

/**
 * Manages loading and parameterization of batch scripts.
 */
public class BatchScript
{
    List<BatchCommand> rawBatch;

    public BatchScript()
    {
    }

    /**
     * Loads a set of raw commands from an input stream.
     * 
     * @param scriptFile File containing script
     * @throws ReplicatorException
     */
    public void load(File scriptFile) throws ReplicatorException
    {
        FileReader fileReader = null;
        try
        {
            fileReader = new FileReader(scriptFile);
            load(scriptFile, fileReader);
        }
        catch (FileNotFoundException e)
        {
            throw new ReplicatorException("Unable to open load script file: "
                    + scriptFile);
        }
        catch (IOException e)
        {
            throw new ReplicatorException("Unable to read load script file: "
                    + scriptFile, e);
        }
        finally
        {
            if (fileReader != null)
            {
                try
                {
                    fileReader.close();
                }
                catch (IOException e)
                {
                }
            }
        }
    }

    /**
     * Loads a set of raw commands from an input stream.
     * 
     * @param source File containing script
     * @param reader Reader from which to read unparameterized script
     * @throws IOException Thrown if there is an I/O error during reading
     */
    public void load(File source, Reader reader) throws IOException
    {
        rawBatch = new LinkedList<BatchCommand>();
        BufferedReader bufferedReader = new BufferedReader(reader);
        StringBuffer nextCommand = new StringBuffer();
        BatchCommand nextBatchCommand = null;
        String nextLine;
        int lineNumber = 0;
        while ((nextLine = bufferedReader.readLine()) != null)
        {
            lineNumber++;
            if (nextLine.trim().length() == 0)
            {
                // Ignore empty lines.
                continue;
            }
            else if (nextLine.charAt(0) == '#')
            {
                // Ignore comments.
                continue;
            }
            else if (Character.isWhitespace(nextLine.charAt(0)))
            {
                // Append indented lines to current command
                // with CR converted to space.
                nextCommand.append(" ").append(nextLine);
            }
            else
            {
                // Anything else is the start of a new line. Dump and
                // parameterize current line, then start a new one.
                if (nextCommand.length() > 0)
                {
                    nextBatchCommand.setRawCommand(nextCommand.toString());
                    rawBatch.add(nextBatchCommand);
                }

                nextCommand = new StringBuffer(nextLine);
                nextBatchCommand = new BatchCommand();
                nextBatchCommand.setFile(source);
                nextBatchCommand.setLineNumber(lineNumber);
            }
        }

        // If we have a line pending at the end, don't forget it.
        if (nextBatchCommand != null)
        {
            nextBatchCommand.setRawCommand(nextCommand.toString());
            rawBatch.add(nextBatchCommand);
        }
    }

    /**
     * Returns a command script with parameters assigned.
     * 
     * @param parameters Map containing parameters as name/value pairs
     * @return Ordered list of parameterized commands
     */
    public List<BatchCommand> getParameterizedScript(
            Map<String, String> parameters)
    {
        List<BatchCommand> batchCommands = new ArrayList<BatchCommand>(
                this.rawBatch.size());
        for (BatchCommand bc : rawBatch)
        {
            String rawCommand = bc.getRawCommand();
            for (String key : parameters.keySet())
            {
                String value = parameters.get(key);
                rawCommand = rawCommand.replace(key, value);
            }
            bc.setCommand(rawCommand);
            batchCommands.add(bc);
        }
        return batchCommands;
    }
}