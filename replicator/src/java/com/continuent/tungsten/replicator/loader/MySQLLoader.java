package com.continuent.tungsten.replicator.loader;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.plugin.PluginContext;

public class MySQLLoader extends JdbcLoader
{
    private static Logger          logger = Logger.getLogger(MySQLLoader.class);
    
    /**
     * 
     * Build a MySQL JDBC connection string
     * 
     * {@inheritDoc}
     * @see com.continuent.tungsten.replicator.loader.JdbcLoader#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException, 
        InterruptedException
    {
        if (url == null)
        {
            StringBuffer sb = new StringBuffer();
            sb.append("jdbc:mysql:thin://");
            sb.append(uri.getHost());
            if (uri.getPort() > 0)
            {
                sb.append(":");
                sb.append(uri.getPort());
            }
            sb.append("/");
            if (uri.getPath() != null)
                sb.append(uri.getPath());
            if (uri.getQuery() != null)
                sb.append(uri.getQuery());

            url = sb.toString();
        }
        else if (logger.isDebugEnabled())
            logger.debug("Property url already set; ignoring host and port properties");
        
        super.configure(context);
    }
    
    public void prepare(PluginContext context) throws ReplicatorException,
        InterruptedException
    {
        super.prepare(context);
    }
    
    /**
     * 
     * Use the output of SHOW MASTER STATUS to get the extractor Event ID
     * 
     * {@inheritDoc}
     * @see com.continuent.tungsten.replicator.extractor.RawExtractor#getCurrentResourceEventId()
     */
    @Override
    public String getCurrentResourceEventId() throws ReplicatorException,
            InterruptedException
    {
        ResultSet masterStatus = null;
        
        try
        {
            masterStatus = statement.executeQuery("SHOW MASTER STATUS");
            
            if (masterStatus.next())
            {
                String fileName = masterStatus.getString("File");
                int dotIndex = fileName.indexOf('.');
                if (dotIndex == -1)
                {
                    throw new ReplicatorException("There was a problem parsing the MASTER STATUS filename");
                }
                
                return fileName.substring(dotIndex+1) + ":" + masterStatus.getString("Position");
            }
            else
            {
                throw new ReplicatorException("Unable to determine the current event id");
            }
        }
        catch (SQLException e)
        {
            throw new ReplicatorException(e);
        }
        finally
        {
            try
            {
                masterStatus.close();
            }
            catch (SQLException e)
            {
                throw new ReplicatorException(e);
            }
        }
    }
}
