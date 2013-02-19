/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2011 Continuent Inc.
 * Contact: tungsten@continuent.org
 *
 * JavaScript example for JavaScriptFilter.
 *
 * Removes specific metadata items from events.
 * Use case example: heterogeneous replication.
 *
 * Example of how to define one in replicator.properties:
 *
 * replicator.filter.dropmetadata=com.continuent.tungsten.replicator.filter.JavaScriptFilter
 * replicator.filter.dropmetadata.script=${replicator.home.dir}/samples/extensions/javascript/dropmetadata.js
 * replicator.filter.dropmetadata.option=service
 *
 * Initial developer(s): Linas Virbalas
 * Contributor(s): 
 */
 
/**
 * Called on every filtered event. See replicator's javadoc for more details
 * on accessible classes. Also, JavaScriptFilter's javadoc contains description
 * about how to define a script like this.
 *
 * @param event Filtered com.continuent.tungsten.replicator.event.ReplDBMSEvent
 *
 * @see com.continuent.tungsten.replicator.filter.JavaScriptFilter
 * @see com.continuent.tungsten.replicator.event.ReplDBMSEvent
 * @see com.continuent.tungsten.replicator.dbms.DBMSData
 * @see com.continuent.tungsten.replicator.dbms.StatementData
 * @see com.continuent.tungsten.replicator.dbms.RowChangeData
 * @see com.continuent.tungsten.replicator.dbms.OneRowChange
 * @see com.continuent.tungsten.replicator.dbms.RowChangeData.ActionType
 * @see com.continuent.tungsten.replicator.thl.THLManagerCtrl#printRowChangeData(StringBuilder, RowChangeData, String, boolean, int)
 * @see java.lang.Thread
 * @see org.apache.log4j.Logger
 */

/**
 * Called once when JavaScriptFilter corresponding to this script is prepared.
 */
function prepare()
{
    option = filterProperties.getString("option");
    logger.info("dropmetadata: will be dropping metadata with option name: " + option);
}

function filter(event)
{
    optionName = filterProperties.getString("option");

    // Analyse what this event is holding.
    data = event.getData();
    
    metaData = event.getDBMSEvent().getMetadata();
    for(m = 0; m < metaData.size(); m++)
    {
        option = metaData.get(m);
        if(option.getOptionName().compareTo(optionName)==0)
        {
            logger.debug("dropmetadata: dropping option: " + option);
            metaData.remove(m);
            logger.debug("dropmetadata: " + event.getDBMSEvent().getMetadata());
            // Job is done, exit the loop.
            break;
        }
    }
    
    // One ReplDBMSEvent may contain many DBMSData events.
    for(i = 0; i < data.size(); i++)
    {
        // Get com.continuent.tungsten.replicator.dbms.DBMSData
        d = data.get(i);
    
        // Determine the underlying type of DBMSData event.
        if(d instanceof com.continuent.tungsten.replicator.dbms.StatementData)
        {
            // It's a SQL statement event.
        }
        else if(d instanceof com.continuent.tungsten.replicator.dbms.RowChangeData)
        {
            // It's a row change event.
        }
    }
}
