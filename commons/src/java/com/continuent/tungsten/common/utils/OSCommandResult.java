
package com.continuent.tungsten.common.utils;

import java.io.Serializable;

public class OSCommandResult implements Serializable
{
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    private String            stdout           = null;
    private String            stderr           = null;
    private int               exitCode         = -1;

    public OSCommandResult(String stdout, String stderr, int exitCode)
    {
        this.stdout = stdout;
        this.stderr = stderr;
        this.exitCode = exitCode;
    }

    public int getExitCode()
    {
        return exitCode;
    }

    public void setExitCode(int exitCode)
    {
        this.exitCode = exitCode;
    }

    public String getStdout()
    {
        return stdout;
    }

    public void setStdout(String stdout)
    {
        this.stdout = stdout;
    }

    public String getStderr()
    {
        return stderr;
    }

    public void setStderr(String stderr)
    {
        this.stderr = stderr;
    }

    public String getMessages()
    {
        StringBuilder builder = new StringBuilder();

        if (stdout != null)
        {
            builder.append(stdout);
        }

        if (stderr != null && stderr.length() > 0)
        {
            if (builder.length() != 0)
                builder.append("\n");

            builder.append(stderr);
        }

        return builder.toString();
    }

    public String toString()
    {
        return String.format("Exit code=%d\n%s", exitCode, getMessages());
    }

}
