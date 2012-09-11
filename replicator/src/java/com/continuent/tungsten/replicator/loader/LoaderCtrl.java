package com.continuent.tungsten.replicator.loader;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.log4j.Logger;

import com.continuent.tungsten.commons.config.TungstenProperties;
import com.continuent.tungsten.commons.exec.ArgvIterator;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntimeConf;
import com.continuent.tungsten.replicator.thl.THLManagerCtrl;
import com.continuent.tungsten.replicator.thl.THLManagerCtrl.InfoHolder;
import com.continuent.tungsten.replicator.loader.csv.CSVLoader;
import com.continuent.tungsten.replicator.thl.log.DiskLog;

public class LoaderCtrl
{
    private static Logger         logger             = Logger.getLogger(LoaderCtrl.class);
    /**
     * Default path to replicator.properties if user not specified other.
     */
    protected static final String defaultConfigPath  = ".."
                                                             + File.separator
                                                             + "conf"
                                                             + File.separator
                                                             + "static-default.properties";

    protected static ArgvIterator argvIterator       = null;

    protected String              configFile         = null;

    private String                logDir;

    private DiskLog               diskLog;

    /**
     * Creates a new <code>THLManagerCtrl</code> object.
     * 
     * @param configFile Path to the Tungsten properties file.
     * @throws Exception
     */
    public LoaderCtrl(String configFile) throws Exception
    {
        // Set path to configuration file.
        this.configFile = configFile;

        // Read properties required to connect to database.
        TungstenProperties properties = readConfig();
        logDir = properties.getString("replicator.store.thl.log_dir");
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
     * TODO: main definition.
     * 
     * @param args
     */
    public static void main(String[] args)
    {
        LoaderCtrl loaderCtrl = null;
        THLManagerCtrl thlManager = null;
        
        try {
            String configFile = null;
            String service = null;
            String loadURI = null;
            
            // Parse command line arguments.
            ArgvIterator argvIterator = new ArgvIterator(args);
            String curArg = null;
            while (argvIterator.hasNext())
            {
                curArg = argvIterator.next();
                if ("-conf".equals(curArg))
                    configFile = argvIterator.next();
                else if ("-service".equals(curArg))
                    service = argvIterator.next();
                else if ("-uri".equals(curArg))
                {
                    loadURI = argvIterator.next();
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
            
            loaderCtrl = new LoaderCtrl(configFile);
            loaderCtrl.prepare();
    
            // Ensure we have a writable log.
            if (!loaderCtrl.diskLog.isWritable())
            {
                println("Fatal error:  The disk log is not writable and cannot be purged.");
                println("If a replication service is currently running, please set the service");
                println("offline first using 'trepctl -service svc offline'");
                fail();
            }
    
            loaderCtrl.loadEvents(loadURI);
            
            loaderCtrl.release();
            
            thlManager = new THLManagerCtrl(configFile);
            thlManager.prepare(true);
            
            InfoHolder info = thlManager.getInfo();
            println("min seq# = " + info.getMinSeqNo());
            println("max seq# = " + info.getMaxSeqNo());
            println("events = " + info.getEventCount());
    
            thlManager.release();
        }
        catch (Throwable t)
        {
            fatal("Fatal error: " + t.getMessage(), t);
        }
        finally
        {
            loaderCtrl.release();
            thlManager.release();
        }
    }
    
    /**
     * Connect to the underlying database containing THL.
     * 
     * @throws ReplicatorException
     */
    public void prepare() throws ReplicatorException,
            InterruptedException
    {
        diskLog = new DiskLog();
        diskLog.setLogDir(logDir);
        diskLog.setReadOnly(false);
        diskLog.prepare();
    }

    /**
     * Disconnect from the THL database.
     */
    public void release()
    {
        if (diskLog != null)
        {
            try
            {
                diskLog.release();
            }
            catch (ReplicatorException e)
            {
                logger.warn("Unable to release log", e);
            }
            catch (InterruptedException e)
            {
                logger.warn("Unexpected interruption while closing log", e);
            }
            diskLog = null;
        }
    }
    
    public void loadEvents(String uriString) throws Exception
    {   
        Loader loader = null;
        
        try
        {
            URI uri = new URI(uriString);
            
            Class<Loader> loaderClass = (Class<Loader>) Class.forName(uri.getScheme());
            loader = (Loader)loaderClass.newInstance();
            loader.setURI(uri);
            loader.setTHL(diskLog);
            loader.prepare();
            
            loader.loadEvents();
        }
        catch (URISyntaxException use)
        {
            throw new Exception("Unable to parse " + uriString);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            logger.warn("Import operation was interrupted!" + e.getMessage());
        }
        finally
        {
            loader.release();
        }
        
        logger.info("Tables imported");
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
    
    private static void printHelp()
    {
        // TODO Auto-generated method stub
        
    }

    /**
     * Appends a message to a given stringBuilder, adds a newline character at
     * the end.
     * 
     * @param msg String to print.
     * @param stringBuilder StringBuilder object to add a message to.
     */
    private static void println(StringBuilder stringBuilder, String msg)
    {
        stringBuilder.append(msg);
        stringBuilder.append("\n");
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
