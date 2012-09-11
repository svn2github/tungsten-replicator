package com.continuent.tungsten.replicator.loader.csv;

import java.io.File;
import java.io.FileReader;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import au.com.bytecode.opencsv.CSVReader;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnSpec;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnVal;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.dbms.RowChangeData.ActionType;
import com.continuent.tungsten.replicator.event.DBMSEmptyEvent;
import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.loader.Loader;
import com.continuent.tungsten.replicator.thl.THLEvent;
import com.continuent.tungsten.replicator.thl.log.LogConnection;

public class CSVLoader extends Loader
{
    private static Logger         logger             = Logger.getLogger(CSVLoader.class);
    
    ArrayList<File> importTables = new ArrayList<File>();
    LogConnection conn = null;

    @Override
    public String getSourceID() throws Exception
    {
        return this.getURI().getHost();
    }

    @Override
    public String getEventID() throws Exception
    {
        List<String> values = this.params.get("eventid");
        if (values != null)
        {
            return values.get(0);
        }
        else
        {
            throw new Exception("Unable to determine the final event id");
        }
    }
    
    protected String getDefaultSchema() throws Exception
    {
        List<String> values = this.params.get("schema");
        if (values != null)
        {
            return values.get(0);
        }
        else
        {
            throw new Exception("Unable to determine the schema");
        }
    }

    public void loadEvents() throws Exception
    {
        DBMSEvent dbmsEvent = null;
        ReplDBMSEvent replDbmsEvent = null;
        ArrayList<DBMSData> dbmsEventData = null;
        RowChangeData rowChangeData = null;
        THLEvent thlEvent = null;
        CSVReader columnReader = null;
        CSVReader rowReader = null;
        ColumnSpec cSpec = null;
        String[] columnDef = null;
        ArrayList<ColumnSpec> columns = null;
        String[] rowDef = null;
        OneRowChange orc = null;
        OneRowChange specOrc = new OneRowChange();
        ArrayList<ColumnVal> columnValues = null;
        ColumnVal cVal = null;
        
        for (Iterator<File> iteratorImportTables = importTables.iterator(); iteratorImportTables.hasNext();)
        {
            File f = iteratorImportTables.next();
            String tableName = f.getName().substring(0, f.getName().length()-4);
            logger.info("Import data for " + tableName);
            
            columns = new ArrayList<ColumnSpec>();
            columnReader = new CSVReader(new FileReader(f), ',', '"');
            while ((columnDef = columnReader.readNext()) != null)
            {
                if (columnDef.length < 2)
                {
                    throw new Exception("The column definition is not formatted properly");
                }
                
                cSpec = specOrc.new ColumnSpec();
                cSpec.setName(columnDef[0]);
                cSpec.setType(new Integer(columnDef[1]));
                
                if (columnDef.length == 3)
                {
                    cSpec.setLength(new Integer(columnDef[2]));
                }
                columns.add(cSpec);
            }
            
            dbmsEventData = new ArrayList<DBMSData>();
            
            rowChangeData = new RowChangeData();

            orc = new OneRowChange();
            orc.setAction(ActionType.INSERT);
            orc.setSchemaName(this.getDefaultSchema());
            orc.setTableName(tableName);
            orc.setColumnSpec(columns);
            
            int numRows = 0;
            
            rowReader = new CSVReader(new FileReader(f.getParent() + "/" + tableName + ".txt"), ',', '"');
            while ((rowDef = rowReader.readNext()) != null)
            {   
                columnValues = new ArrayList<ColumnVal>();
                
                for (int i=0; i < rowDef.length; i++)
                {
                    cVal = orc.new ColumnVal();
                    cVal.setValue(this.parseStringValue(columns.get(i).getType(), rowDef[i]));
                    
                    columnValues.add(cVal);
                }
                orc.getColumnValues().add(columnValues);
                
                numRows++;
            }
            
            rowChangeData.appendOneRowChange(orc);
            dbmsEventData.add(rowChangeData);
            
            dbmsEvent = new DBMSEvent("import-" + tableName, null, 
                    dbmsEventData, true, null);
            replDbmsEvent = new ReplDBMSEvent(this.thl.getMaxSeqno()+1, 
                    dbmsEvent);
            thlEvent = new THLEvent("import-" + tableName, replDbmsEvent);
            conn.store(thlEvent, true);
            
            logger.info(numRows + " rows loaded");
        }
        
        replDbmsEvent = new ReplDBMSEvent(this.thl.getMaxSeqno()+1, 
                (short) 0, true, getSourceID(), 0, new Timestamp(
                System.currentTimeMillis()), new DBMSEmptyEvent(getEventID()));
        thlEvent = new THLEvent(replDbmsEvent.getEventId(), replDbmsEvent);
        conn.store(thlEvent, true);
    }
    
    public void prepare() throws Exception
    {
        logger.info("Import tables from " + this.uri.getPath() + " to the " + this.uri.getScheme() + " schema");

        File importDirectory = new File(this.uri.getPath());
        if (!importDirectory.exists())
        {
            throw new ReplicatorException("The " + this.uri.getPath() + " directory does not exist");
        }
        
        for (File f : importDirectory.listFiles())
        {
            if (f.getName().endsWith(".def"))
            {
                importTables.add(f);
            }
        }
        
        this.conn = this.thl.connect(false);
    }
    
    public void release() throws Exception
    {
        if (this.thl != null)
        {
            this.thl.release();
        }
    }
}
