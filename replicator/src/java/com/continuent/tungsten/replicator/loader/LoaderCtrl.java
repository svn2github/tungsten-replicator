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
 * Initial developer(s): Jeff Mace
 */

package com.continuent.tungsten.replicator.loader;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.common.exec.ArgvIterator;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.conf.ReplicatorMonitor;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntime;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntimeConf;
import com.continuent.tungsten.replicator.extractor.ExtractorWrapper;
import com.continuent.tungsten.replicator.management.MockEventDispatcher;
import com.continuent.tungsten.replicator.management.MockOpenReplicatorContext;
import com.continuent.tungsten.replicator.pipeline.Pipeline;
import com.continuent.tungsten.replicator.storage.Store;
import com.continuent.tungsten.replicator.thl.THL;
import com.continuent.tungsten.replicator.thl.THLManagerCtrl;

public class LoaderCtrl
{
    private static Logger         logger            = Logger.getLogger(LoaderCtrl.class);
    /**
     * Default path to replicator.properties if user not specified other.
     */
    protected static final String defaultConfigPath = ".."
                                                            + File.separator
                                                            + "conf"
                                                            + File.separator
                                                            + "static-default.properties";

    protected static ArgvIterator argvIterator      = null;

    protected String              configFile        = null;

    protected TungstenProperties  loaderProperties  = null;

    /**
     * Creates a new <code>THLManagerCtrl</code> object.
     * 
     * @param configFile Path to the Tungsten properties file.
     * @throws Exception
     */
    public LoaderCtrl(String configFile, TungstenProperties loaderProperties)
            throws Exception
    {
        // Set path to configuration file.
        this.configFile = configFile;
        this.loaderProperties = loaderProperties;
    }

    /**
     * Reads the replicator.properties.
     */
    protected TungstenProperties readConfig() throws Exception
    {
        TungstenProperties conf = null;

        // Open configuration file.
        File propsFile = new File(configFile);
        if (!propsFile.exists() || !propsFile.canRead())
        {
            throw new Exception("Properties file not found: "
                    + propsFile.getAbsolutePath(), null);
        }
        conf = new TungstenProperties();

        // Read configuration.
        try
        {
            conf.load(new FileInputStream(propsFile));
        }
        catch (IOException e)
        {
            throw new Exception(
                    "Unable to read properties file: "
                            + propsFile.getAbsolutePath() + " ("
                            + e.getMessage() + ")", null);
        }
        return conf;
    }

    /**
     * Read command line arguments in run the Loader process
     * 
     * @param argv
     */
    public static void main(String[] argv)
    {
        LoaderCtrl loaderCtrl = null;
        THLManagerCtrl thlManager = null;

        try
        {
            String configFile = null;
            String service = null;
            TungstenProperties tempProperties = new TungstenProperties();

            // Parse command line arguments.
            ArgvIterator argvIterator = new ArgvIterator(argv);
            String curArg = null;
            while (argvIterator.hasNext())
            {
                curArg = argvIterator.next();
                if ("-conf".equals(curArg))
                    configFile = argvIterator.next();
                else if ("-service".equals(curArg))
                    service = argvIterator.next();
                else if ("-chunk-size".equals(curArg))
                {
                    tempProperties.setProperty(
                            "replicator.extractor.loader.chunkSize",
                            argvIterator.next());
                }
                else if (curArg.startsWith("-"))
                {
                    String key = curArg.substring(1);
                    String curValue = argvIterator.next();

                    if ("extractor".equals(key))
                    {
                        key = "replicator.extractor.loader";
                    }
                    else if (key.startsWith("extractor."))
                    {
                        key = "replicator.extractor.loader."
                                + key.substring(10);
                    }

                    tempProperties.setProperty(key, curValue);
                }
                else
                    fatal("Unrecognized option: " + curArg, null);
            }

            // Use default configuration file in case user didn't specify one.
            if (configFile == null)
            {
                if (service == null)
                {
                    configFile = lookForConfigFile();
                    if (configFile == null)
                    {
                        fatal("You must specify either a config file or a service name (-conf or -service)",
                                null);
                    }
                }
                else
                {
                    ReplicatorRuntimeConf runtimeConf = ReplicatorRuntimeConf
                            .getConfiguration(service);
                    configFile = runtimeConf.getReplicatorProperties()
                            .getAbsolutePath();
                }
            }

            loaderCtrl = new LoaderCtrl(configFile, tempProperties);
            loaderCtrl.prepare();

            loaderCtrl.loadEvents();
        }
        catch (Throwable t)
        {
            t.printStackTrace();
            fatal("Fatal error: " + t.getMessage(), t);
        }
        finally
        {
            if (loaderCtrl != null)
            {
                loaderCtrl.release();
            }

            if (thlManager != null)
            {
                thlManager.release();
            }
        }

        succeed();
    }

    /**
     * Connect to the underlying database containing THL.
     * 
     * @throws ReplicatorException
     */
    public void prepare() throws ReplicatorException, InterruptedException
    {
    }

    /**
     * Disconnect from the THL database.
     */
    public void release()
    {
    }

    protected TungstenProperties buildLoaderConfig() throws Exception
    {
        TungstenProperties conf = this.readConfig();
        conf.putAll(loaderProperties);

        String role = conf.getProperty("replicator.role");

        if (role.equals("master"))
        {
            conf.setProperty("replicator.stage.binlog-to-q.extractor", "loader");
        }
        else if (role.equals("slave"))
        {
            conf.setProperty("replicator.pipeline.slave",
                    "loader-to-q,q-to-dbms");
            conf.setProperty("replicator.pipeline.slave.stores", "queue");
        }
        else if (role == "direct")
        {
            conf.setProperty("replicator.pipeline.direct",
                    "loader-to-q,q-to-dbms");
            conf.setProperty("replicator.pipeline.direct.stores", "queue");
        }

        conf.setProperty("replicator.stage.loader-to-q",
                "com.continuent.tungsten.replicator.pipeline.SingleThreadStageTask");
        conf.setProperty("replicator.stage.loader-to-q.extractor", "loader");
        conf.setProperty("replicator.stage.loader-to-q.applier", "queue");
        conf.setProperty("replicator.stage.loader-to-q.blockCommitRowCount",
                "${replicator.global.buffer.size}");
        conf.setProperty("replicator.stage.q-to-dbms.extractor", "queue");

        // Substitute ${..} values
        Properties props = conf.getProperties();
        TungstenProperties.substituteSystemValues(props, 10);
        conf.load(props);

        return conf;
    }

    public void loadEvents() throws Exception
    {
        ReplicatorRuntime runtime = null;
        Pipeline pipeline = null;
        Class<?> headExtractorClass = null;
        Class<?> storeClass = null;

        try
        {
            runtime = new ReplicatorRuntime(this.buildLoaderConfig(),
                    new MockOpenReplicatorContext(),
                    ReplicatorMonitor.getInstance());
            runtime.configure();
            pipeline = runtime.getPipeline();

            ExtractorWrapper ew = (ExtractorWrapper) pipeline
                    .getHeadExtractor();
            headExtractorClass = ew.getExtractor().getClass();
            if (Loader.class.isAssignableFrom(headExtractorClass) != true)
            {
                throw new Exception("Unable to start the loader because "
                        + headExtractorClass + " does not extend "
                        + Loader.class);
            }

            Loader extractor = (Loader) ew.getExtractor();
            if (extractor.getLockTables() == true)
            {
                for (String storeName : pipeline.getStoreNames())
                {
                    Store store = pipeline.getStore(storeName);
                    storeClass = store.getClass();
                    if (THL.class.isAssignableFrom(storeClass) == true)
                    {
                        THL thlStore = (THL) store;
                        thlStore.setUrl(null);
                    }
                }
            }

            runtime.prepare();
            pipeline.start(new MockEventDispatcher());
            pipeline.shutdownAfterHeartbeat("LOAD_COMPLETE");
            while (pipeline.isShutdown() != true)
            {
                // Wait for the pipeline to complete
                Thread.sleep(100);
            }

            logger.info("Tables imported");
        }
        finally
        {
            runtime.release();
        }
    }

    // Return the service configuration file if there is one
    // and only one file that matches the static-svcname.properties pattern.
    private static String lookForConfigFile()
    {
        File configDir = ReplicatorRuntimeConf.locateReplicatorConfDir();
        FilenameFilter propFileFilter = new FilenameFilter()
        {
            public boolean accept(File fdir, String fname)
            {
                if (fname.startsWith("static-")
                        && fname.endsWith(".properties"))
                    return true;
                else
                    return false;
            }
        };
        File[] propertyFiles = configDir.listFiles(propFileFilter);
        if (propertyFiles.length == 1)
            return propertyFiles[0].getAbsolutePath();
        else
            return null;
    }

    /**
     * Print a message to stdout with trailing new line character.
     * 
     * @param msg
     */
    protected static void println(String msg)
    {
        System.out.println(msg);
    }

    /**
     * Print a message to stdout without trailing new line character.
     * 
     * @param msg
     */
    protected static void print(String msg)
    {
        System.out.print(msg);
    }

    /**
     * Abort following a fatal error.
     * 
     * @param msg
     * @param t
     */
    protected static void fatal(String msg, Throwable t)
    {
        System.out.println(msg);
        if (t != null)
            t.printStackTrace();
        fail();
    }

    /**
     * Exit with a process failure code.
     */
    protected static void fail()
    {
        System.exit(1);
    }

    /**
     * Exit with a process success code.
     */
    protected static void succeed()
    {
        System.exit(0);
    }
}
