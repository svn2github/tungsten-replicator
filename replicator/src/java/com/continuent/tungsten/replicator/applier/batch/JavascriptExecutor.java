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
 * Contributor(s): Linas Virbalas
 */

package com.continuent.tungsten.replicator.applier.batch;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;

import org.apache.log4j.Logger;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.ContextFactory;
import org.mozilla.javascript.EvaluatorException;
import org.mozilla.javascript.Function;
import org.mozilla.javascript.Script;
import org.mozilla.javascript.Scriptable;
import org.mozilla.javascript.ScriptableObject;
import org.mozilla.javascript.Undefined;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * Represents a class to execute a Javascript batch load script. This class is a
 * partial copy/paste from logic in class JavaScriptFilter, which integrates
 * Rhino Javascript for filters.
 * 
 * @see com.continuent.tungsten.replicator.filter.JavaScriptFilter
 */
public class JavascriptExecutor implements ScriptExecutor
{
    private static Logger logger        = Logger.getLogger(JavascriptExecutor.class);

    // Location of script.
    private String        scriptFile    = null;

    // DBMS connection and statement.
    private Database      connection;
    private SqlWrapper    connectionWrapper;

    // Compiled user's script.
    private Script        script        = null;

    // JavaScript scope containing all objects including functions of the user's
    // script and our exported objects.
    private Scriptable    scope         = null;

    // Pointer to the script's apply function.
    private Function      applyFunction = null;

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.batch.ScriptExecutor#setConnection(com.continuent.tungsten.replicator.database.Database)
     */
    @Override
    public void setConnection(Database connection)
    {
        this.connection = connection;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.batch.ScriptExecutor#setScript(java.lang.String)
     */
    @Override
    public void setScript(String script)
    {
        this.scriptFile = script;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.batch.ScriptExecutor#setShowCommands(boolean)
     */
    @Override
    public void setShowCommands(boolean showCommands)
    {
        // Does nothing for now...
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        // Determine which JS script to use.
        if (scriptFile == null)
            throw new ReplicatorException(
                    "scriptFile property must be set for JavaScript applier to work");
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.batch.ScriptExecutor#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    @Override
    public void prepare(PluginContext context) throws ReplicatorException
    {
        // Create a connection wrapper to provide SQL capabilities.
        try
        {
            connectionWrapper = new SqlWrapper(connection);
        }
        catch (SQLException e)
        {
            throw new ReplicatorException(
                    "Unable to initialize JDBC connection for load script: script="
                            + script + " message=" + e.getMessage(), e);
        }

        // Create JavaScript context which will be used for preparing script.
        Context jsContext = ContextFactory.getGlobal().enterContext();

        // Create script's scope.
        scope = jsContext.initStandardObjects();

        // Compile user's JavaScript files for future usage, so they wouldn't
        // require compilation on every filtered event.
        try
        {
            // Read and compile the script.
            BufferedReader in = new BufferedReader(new FileReader(scriptFile));
            script = jsContext.compileReader(in, scriptFile, 0, null);
            in.close();

            // Execute script to get functions into scope.
            script.exec(jsContext, scope);

            // Provide access to the logger object.
            ScriptableObject.putProperty(scope, "logger", logger);

            // Provide access to the SQL connection wrapper.
            ScriptableObject.putProperty(scope, "sql", connectionWrapper);
            
            // Provide access to a runtime to help run processes and other useful things. 
            ScriptableObject.putProperty(scope, "runtime", new JavascriptRuntime());

            // Get a pointer to function "apply(info)".
            Object filterObj = scope.get("apply", scope);
            if (!(filterObj instanceof Function))
                logger.error("apply(info) is undefined in " + scriptFile);
            else
                applyFunction = (Function) filterObj;

            // Get a pointer to function "prepare()" and call it.
            getFunctionAndCall(jsContext, "prepare");
        }
        catch (IOException e)
        {
            throw new ReplicatorException("Script file not found: "
                    + scriptFile, e);
        }
        catch (EvaluatorException e)
        {
            throw new ReplicatorException(e);
        }
        finally
        {
            // Exit JavaScript context.
            Context.exit();
        }

    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.batch.ScriptExecutor#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    @Override
    public void release(PluginContext context)
    {
        // Release SQL resources.
        if (connectionWrapper != null)
        {
            connectionWrapper.close();
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.batch.ScriptExecutor#execute(com.continuent.tungsten.replicator.applier.batch.CsvInfo)
     */
    @Override
    public void execute(CsvInfo info) throws ReplicatorException
    {
        // Call script if it was successfully prepared.
        if (applyFunction != null)
        {
            // We are in a method which might be called from a different thread
            // than the one that called the prepare() method. Thus we need to
            // enter JavaScript context.
            Context jsContext = ContextFactory.getGlobal().enterContext();

            // Provide access to current thread object.
            ScriptableObject.putProperty(scope, "thread",
                    Thread.currentThread());

            // Call function "filter(event)" and log its result if one was
            // returned.
            Object functionArgs[] = {info};
            applyFunction.call(jsContext, scope, scope, functionArgs);

            // Exit JavaScript context.
            Context.exit();
        }
    }

    /**
     * Tries to get a pointer to a function in the script and call it. If
     * unsuccessful, logs a message into DEBUG stream about failure. If
     * successful and function returns a value, this method logs string
     * representation of the return value into INFO stream.
     * 
     * @param functionName Function name to get and call.
     * @return true, if function called, false, otherwise.
     */
    private boolean getFunctionAndCall(Context jsContext, String functionName)
    {
        Object fObj = scope.get(functionName, scope);
        if (!(fObj instanceof Function))
        {
            logger.debug(functionName + "() is undefined in " + scriptFile);
            return false;
        }
        else
        {
            Function f = (Function) fObj;
            Object result = f.call(jsContext, scope, scope, null);
            logIfDefined(result);
            return true;
        }
    }

    /**
     * Logs object's Context.toString(...) representation to INFO stream if it's
     * defined.
     * 
     * @param objToLog Object which string representation to log.
     * @return true, if logged, false, if object was undefined or null.
     */
    private boolean logIfDefined(Object objToLog)
    {
        if (objToLog != null && !(objToLog instanceof Undefined))
        {
            logger.info(Context.toString(objToLog));
            return true;
        }
        else
            return false;
    }
}