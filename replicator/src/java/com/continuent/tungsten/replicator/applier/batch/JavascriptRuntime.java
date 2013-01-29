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

package com.continuent.tungsten.replicator.applier.batch;

import java.util.Map;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.exec.ProcessExecutor;
import com.continuent.tungsten.replicator.ReplicatorException;

/**
 * Implements a runtime that can be provided to Javascript scripts with useful
 * functions like launching an OS process or failing with an exception.
 */
public class JavascriptRuntime
{
    private static Logger logger = Logger.getLogger(JavascriptRuntime.class);

    /**
     * Execute an OS command and return the result of stdout.
     * 
     * @param command Command to run
     * @return Returns output of the command in a string
     * @throws ReplicatorException Thrown if command execution fails
     */
    public String exec(String command) throws ReplicatorException
    {
        String[] osArray = {"sh", "-c", command};
        ProcessExecutor pe = new ProcessExecutor();
        pe.setCommands(osArray);
        if (logger.isDebugEnabled())
        {
            logger.debug("Executing OS command: " + command);
        }
        pe.run();
        if (logger.isDebugEnabled())
        {
            logger.debug("OS command stdout: " + pe.getStdout());
            logger.debug("OS command stderr: " + pe.getStderr());
            logger.debug("OS command exit value: " + pe.getExitValue());
        }
        if (!pe.isSuccessful())
        {
            logger.error("OS command failed: command=" + command + " rc="
                    + pe.getExitValue() + " stdout=" + pe.getStdout()
                    + " stderr=" + pe.getStderr());
            throw new ReplicatorException("OS command failed: command="
                    + command);
        }
        return pe.getStdout();
    }

    /**
     * Substitutes parameter values from a Map into a command. This is used to
     * apply %%PARM%% style-parameters to a command template.
     * 
     * @param command Command template with parameter names
     * @param parameters Map containing name value pairs of parameters
     * @return Fully parameter
     */
    public String parameterize(String command,
            Map<String, String> parameters)
    {
        for (String key : parameters.keySet())
        {
            String value = parameters.get(key);
            command = command.replace(key, value);
        }
        return command;
    }

    /**
     * Supplies equivalent of sprintf function for Javascript callers to use.
     * 
     * @param format Printf-style format string
     * @param args Varargs values to substitute into the format.
     * @return Formatted string
     */
    public String sprintf(String format, Object... args)
    {
        return String.format(format, args);
    }
}