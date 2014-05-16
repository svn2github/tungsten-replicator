/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2014 Continuent Inc.
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
 * Initial developer(s): Linas Virbalas
 * Contributor(s): 
 */

package com.continuent.tungsten.replicator.filter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.conf.ReplicatorConf;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnSpec;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnVal;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * Filter which sends column values to a TCP server for processing, receives the
 * processed results and uses it as a new value.
 * 
 * @author <a href="mailto:linas.virbalas@continuent.com">Linas Virbalas</a>
 * @version 1.0
 */
public class NetworkClientFilter implements Filter
{
    private static Logger          logger               = Logger.getLogger(NetworkClientFilter.class);

    /**
     * Path to definition file.
     */
    private String                 definitionsFile      = null;

    /**
     * TCP port filtering server is listening on.
     */
    private int                    serverPort           = 3112;

    /**
     * Parsed JSON holder.
     */
    private Map<String, JSONArray> definitions          = null;

    /**
     * Count of column entries in the definitions file.
     */
    private int                    definedColumnEntries = 0;

    /**
     * Name of current replication service's internal tungsten schema.
     */
    private String                 tungstenSchema;

    /**
     * Parser used to read column definition file.
     */
    private static JSONParser      parser               = new JSONParser();

    private Socket                 socket               = null;
    private PrintWriter            toServer             = null;
    private BufferedReader         fromServer           = null;

    private ClientMessageGenerator messageGenerator     = null;

    /**
     * Sets the path to definition file.
     * 
     * @param definitionsFile Path to file.
     */
    public void setDefinitionsFile(String definitionsFile)
    {
        this.definitionsFile = definitionsFile;
    }

    /**
     * Set TCP port that filtering server is listening on.
     */
    public void setServerPort(int serverPort)
    {
        this.serverPort = serverPort;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.filter.Filter#filter(com.continuent.tungsten.replicator.event.ReplDBMSEvent)
     */
    public ReplDBMSEvent filter(ReplDBMSEvent event) throws ReplicatorException
    {
        ArrayList<DBMSData> data = event.getData();
        for (Iterator<DBMSData> iterator = data.iterator(); iterator.hasNext();)
        {
            DBMSData dataElem = iterator.next();
            if (dataElem instanceof RowChangeData)
            {
                RowChangeData rdata = (RowChangeData) dataElem;
                for (Iterator<OneRowChange> iterator2 = rdata.getRowChanges()
                        .iterator(); iterator2.hasNext();)
                {
                    OneRowChange orc = iterator2.next();

                    // Don't analyze tables from Tungsten schema.
                    if (orc.getSchemaName().compareToIgnoreCase(tungstenSchema) == 0)
                    {
                        if (logger.isDebugEnabled())
                            logger.debug("Ignoring " + tungstenSchema
                                    + " schema");
                        continue;
                    }

                    // Loop through defined transformations (usually one).
                    Iterator<String> it = definitions.keySet().iterator();
                    while (it.hasNext())
                    {
                        String transformation = it.next();

                        JSONArray array = (JSONArray) definitions
                                .get(transformation);
                        // Not using any hash, because simple loop should be
                        // fast enough for a few expected entries.
                        for (Object o : array)
                        {
                            JSONObject jo = (JSONObject) o;
                            String defSchema = (String) jo.get("schema");
                            String defTable = (String) jo.get("table");

                            // Found a filter request for this schema & table?
                            if ((defSchema.equals("*") || defSchema.equals(orc
                                    .getSchemaName()))
                                    && (defTable.equals("*") || defTable
                                            .equals(orc.getTableName())))
                            {
                                // Defined columns to filter.
                                JSONArray defColumns = (JSONArray) jo
                                        .get("columns");

                                // Filter column values.
                                ArrayList<ColumnSpec> colSpecs = orc
                                        .getColumnSpec();
                                ArrayList<ArrayList<OneRowChange.ColumnVal>> colValues = orc
                                        .getColumnValues();
                                for (int c = 0; c < colSpecs.size(); c++)
                                {
                                    ColumnSpec colSpec = colSpecs.get(c);
                                    if (colSpec.getName() != null)
                                    {
                                        // Have this column in definitions?
                                        if (defColumns.contains(colSpec
                                                .getName()))
                                        {
                                            // Iterate through all rows in the
                                            // column.
                                            for (int row = 0; row < colValues
                                                    .size(); row++)
                                            {
                                                ColumnVal colValue = colValues
                                                        .get(row).get(c);
                                                if (colValue.getValue() != null)
                                                {
                                                    if (logger.isDebugEnabled())
                                                        logger.debug("Sending value: "
                                                                + colValue
                                                                        .getValue());

                                                    // Send to server.
                                                    String newValue = sendToFilter(
                                                            transformation,
                                                            event.getSeqno(),
                                                            row,
                                                            orc.getSchemaName(),
                                                            orc.getTableName(),
                                                            colSpec.getName(),
                                                            colValue.getValue());
                                                    colValue.setValue(newValue);

                                                    if (logger.isDebugEnabled())
                                                        logger.debug("Received value: "
                                                                + newValue);
                                                }
                                            }
                                        }
                                    }
                                    else
                                    {
                                        if (logger.isDebugEnabled())
                                        {
                                            logger.debug("Expected to filter column, but column name is undefined: "
                                                    + orc.getSchemaName()
                                                    + "."
                                                    + orc.getTableName()
                                                    + "["
                                                    + colSpec.getIndex() + "]");
                                        }
                                    }
                                }

                                // Filter key values.
                                ArrayList<ColumnSpec> keySpecs = orc
                                        .getKeySpec();
                                ArrayList<ArrayList<OneRowChange.ColumnVal>> keyValues = orc
                                        .getKeyValues();
                                for (int k = 0; k < keySpecs.size(); k++)
                                {
                                    ColumnSpec keySpec = keySpecs.get(k);
                                    if (keySpec.getName() != null)
                                    {
                                        if (defColumns.contains(keySpec
                                                .getName()))
                                        {
                                            // Iterate through all rows in the
                                            // key.
                                            for (int row = 0; row < keyValues
                                                    .size(); row++)
                                            {
                                                ColumnVal keyValue = keyValues
                                                        .get(row).get(k);
                                                if (keyValue.getValue() != null)
                                                {
                                                    if (logger.isDebugEnabled())
                                                        logger.debug("Sending value: "
                                                                + keyValue
                                                                        .getValue());

                                                    // Send to server.
                                                    String newValue = sendToFilter(
                                                            transformation,
                                                            event.getSeqno(),
                                                            row,
                                                            orc.getSchemaName(),
                                                            orc.getTableName(),
                                                            keySpec.getName(),
                                                            keyValue.getValue());
                                                    keyValue.setValue(newValue);

                                                    if (logger.isDebugEnabled())
                                                        logger.debug("Received value: "
                                                                + newValue);
                                                }
                                            }
                                        }
                                    }
                                    else
                                    {
                                        if (logger.isDebugEnabled())
                                        {
                                            logger.debug("Expected to filter key, but column name is undefined: "
                                                    + orc.getSchemaName()
                                                    + "."
                                                    + orc.getTableName()
                                                    + "["
                                                    + keySpec.getIndex() + "]");
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            else if (dataElem instanceof StatementData)
            {
                // Not supported.
            }
        }
        return event;
    }

    /**
     * Sets the Tungsten schema, which we ignore to prevent problems with the
     * replicator. This is mostly used for filter testing, which runs without a
     * pipeline.
     */
    public void setTungstenSchema(String tungstenSchema)
    {
        this.tungstenSchema = tungstenSchema;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException
    {
        if (tungstenSchema == null)
        {
            tungstenSchema = context.getReplicatorProperties().getString(
                    ReplicatorConf.METADATA_SCHEMA);
        }
        if (definitionsFile == null)
        {
            throw new ReplicatorException(
                    "definitionsFile property not set - specify a path to JSON file");
        }
    }

    /**
     * Reads the whole text file into a String.
     * 
     * @throws Exception if the file cannot be found.
     */
    private String readDefinitionsFile(String file) throws IOException
    {
        BufferedReader br = new BufferedReader(new FileReader(file));
        try
        {
            StringBuilder builder = new StringBuilder();
            String line = null;
            String newLine = System.getProperty("line.separator");
            while ((line = br.readLine()) != null)
            {
                builder.append(line);
                builder.append(newLine);
            }
            return builder.toString();
        }
        finally
        {
            try
            {
                if (br != null)
                    br.close();
            }
            catch (IOException ex)
            {
                logger.warn("Unable to close file " + file + ": "
                        + ex.toString());
            }
        }
    }

    /**
     * Returns how many different transformations are defined in the JSON
     * definitions file. Usually that's one, unless server supports multiple
     * transformation.
     */
    public int getDefinedTransformations()
    {
        if (definitions != null)
        {
            return definitions.keySet().size();
        }
        else
        {
            return 0;
        }
    }

    /**
     * Returns how many column entries were parsed out of the JSON file.
     */
    public int getDefinedColumnEntries()
    {
        return definedColumnEntries;
    }

    /**
     * Initial validation of the JSON definitions file.
     */
    private void initDefinitionsFile() throws ReplicatorException
    {
        try
        {
            logger.info("Using: " + definitionsFile);

            String jsonText = readDefinitionsFile(definitionsFile);
            Object obj = parser.parse(jsonText);
            @SuppressWarnings("unchecked")
            Map<String, JSONArray> map = (Map<String, JSONArray>) obj;
            definitions = map;

            Iterator<String> it = definitions.keySet().iterator();
            while (it.hasNext())
            {
                String transformation = it.next();
                logger.info("Transformation: " + transformation);

                JSONArray array = (JSONArray) definitions.get(transformation);
                for (Object o : array)
                {
                    JSONObject jo = (JSONObject) o;
                    String schema = (String) jo.get("schema");
                    String table = (String) jo.get("table");
                    JSONArray columns = (JSONArray) jo.get("columns");
                    logger.info("  In " + schema + "." + table + ": ");
                    for (Object c : columns)
                    {
                        String column = (String) c;
                        definedColumnEntries++;
                        logger.info("    " + column);
                    }
                }
            }
        }
        catch (ClassCastException e)
        {
            throw new ReplicatorException(
                    "Unable to read definitions file (is JSON structure correct?): "
                            + e, e);
        }
        catch (ParseException e)
        {
            throw new ReplicatorException(
                    "Unable to read definitions file (error parsing JSON): "
                            + e, e);
        }
        catch (IOException e)
        {
            throw new ReplicatorException(e);
        }
    }

    /**
     * Connects to the filtering server and prepares I/O streams.
     */
    private void initConnection() throws ReplicatorException
    {
        try
        {
            // Connect to filtering server.
            logger.info("Connecting to the filtering server on port "
                    + serverPort);
            InetAddress host = InetAddress.getByName("localhost");
            socket = new Socket(host, serverPort);
            logger.info("Connected to " + socket.getRemoteSocketAddress());

            toServer = new PrintWriter(socket.getOutputStream(), true);
            fromServer = new BufferedReader(new InputStreamReader(
                    socket.getInputStream()));
        }
        catch (UnknownHostException e)
        {
            throw new ReplicatorException(
                    "Unable to connect to filtering server: " + e, e);
        }
        catch (IOException e)
        {
            throw new ReplicatorException(
                    "Unable to connect to filtering server: " + e, e);
        }
    }

    /**
     * Send prepare message to the server and check that a valid acknowledged
     * message is received.
     */
    private void doHandshake(String contextService) throws ReplicatorException
    {
        try
        {
            // Send prepare message.
            toServer.println(messageGenerator.prepare());

            // Receive & check acknowledged message.
            String message = fromServer.readLine();
            if (logger.isDebugEnabled())
                logger.debug("Received: " + message);

            String payload = NetworkClientFilter.Protocol.getPayload(message);
            String json = NetworkClientFilter.Protocol.getHeader(message);
            JSONObject obj = (JSONObject) parser.parse(json);
            String type = (String) obj.get("type");
            String service = (String) obj.get("service");
            long returnCode = (Long) obj.get("return");

            validateMessage(Protocol.TYPE_ACKNOWLEDGED, type, returnCode,
                    service, payload);

            logger.info("Server: " + payload);
        }
        catch (ParseException e)
        {
            throw new ReplicatorException(
                    "Server returned an invalid message during prepare-acknowledged message handshake: "
                            + e, e);
        }
        catch (IOException e)
        {
            throw new ReplicatorException(
                    "prepare-acknowledged message handshake failed: " + e, e);
        }
    }

    /**
     * Sends column/key value to the server and receives filtered result.
     */
    private String sendToFilter(String transformation, long seqno, int row,
            String schema, String table, String column, Object value)
            throws ReplicatorException
    {
        try
        {
            // Send filter message.
            toServer.println(messageGenerator.filter(transformation, seqno,
                    row, schema, table, column, (String) value));

            // Receive & check filtered message.
            String message = fromServer.readLine();
            if (logger.isDebugEnabled())
                logger.debug("Received: " + message);

            String payload = NetworkClientFilter.Protocol.getPayload(message);
            String json = NetworkClientFilter.Protocol.getHeader(message);
            JSONObject obj = (JSONObject) parser.parse(json);
            String type = (String) obj.get("type");
            long newSeqno = (Long) obj.get("seqno");
            long newRow = (Long) obj.get("row");
            String newSchema = (String) obj.get("schema");
            String newTable = (String) obj.get("table");
            long returnCode = (Long) obj.get("return");
            String service = (String) obj.get("service");

            // Validate that returned information matches what we requested.
            validateMessage(Protocol.TYPE_FILTERED, type, returnCode, service,
                    payload);
            if (newSeqno != seqno)
                throw new ReplicatorException("Expected to receive seqno "
                        + seqno + ", but server sent " + newSeqno
                        + " instead: " + message);
            if (newRow != row)
                throw new ReplicatorException("Expected to receive row " + row
                        + ", but server sent " + newRow + " instead: "
                        + message);
            if (!newSchema.equals(schema))
                throw new ReplicatorException("Expected to receive schema "
                        + schema + ", but server sent " + newSchema
                        + " instead: " + message);
            if (!newTable.equals(table))
                throw new ReplicatorException("Expected to receive table "
                        + table + ", but server sent " + newTable
                        + " instead: " + message);

            return payload;
        }
        catch (ParseException e)
        {
            throw new ReplicatorException(
                    "Server returned an invalid message during prepare-acknowledged message handshake: "
                            + e, e);
        }
        catch (IOException e)
        {
            throw new ReplicatorException(
                    "prepare-acknowledged message handshake failed: " + e, e);
        }
    }

    /**
     * Protocol safety checks for the server returned message.
     */
    private void validateMessage(String expectedType, String type,
            long returnCode, String service, String payload)
            throws ReplicatorException
    {
        if (!type.equals(expectedType))
        {
            throw new ReplicatorException(
                    "Server should have returned message of type \""
                            + Protocol.TYPE_FILTERED + "\", but returned \""
                            + type + "\" instead");
        }
        else if (returnCode != 0)
        {
            throw new ReplicatorException("Server returned a non-zero code ("
                    + returnCode + "), payload: " + payload);
        }
        else if (!service.equals(messageGenerator.getService()))
        {
            throw new ReplicatorException(
                    "Server returned unexpected service name in the message: received \""
                            + service + "\", but expected \""
                            + messageGenerator.getService() + "\"");
        }
    }

    /**
     * Prepares connection to the filtering server and parses definition file.
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context) throws ReplicatorException
    {
        messageGenerator = new ClientMessageGenerator(context.getServiceName());

        initDefinitionsFile();
        initConnection();
        doHandshake(context.getServiceName());
    }

    /**
     * Sends release message to the server. Confirms the success. Failure are
     * logged, but otherwise ignored.
     */
    private void sendRelease()
    {
        try
        {
            // Send release message.
            toServer.println(messageGenerator.release());

            // Receive & check acknowledged message.
            String message = fromServer.readLine();
            if (logger.isDebugEnabled())
                logger.debug("Received: " + message);
            String payload = NetworkClientFilter.Protocol.getPayload(message);
            String json = NetworkClientFilter.Protocol.getHeader(message);
            JSONObject obj = (JSONObject) parser.parse(json);
            String type = (String) obj.get("type");
            long returnCode = (Long) obj.get("return");

            if (type.equals(Protocol.TYPE_ACKNOWLEDGED))
            {
                if (returnCode == 0)
                {
                    logger.info("Server acknowledged filter release: "
                            + payload);
                }
                else
                {
                    logger.warn("Server returned a non-zero code ("
                            + returnCode + ") in response to release message: "
                            + payload);

                }
            }
            else
            {
                logger.warn("Server should have returned message of type \""
                        + Protocol.TYPE_ACKNOWLEDGED + "\", but returned \""
                        + type + "\" instead. Full message: " + message);
            }
        }
        catch (ParseException e)
        {
            logger.warn("Error parsing message received back from the filtering server after release message (ignoring): "
                    + e);
        }
        catch (IOException e)
        {
            logger.warn("Sending of release message to the filtering server failed (ignoring): "
                    + e);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void release(PluginContext context) throws ReplicatorException
    {
        definitions = null;
        definedColumnEntries = 0;

        try
        {
            logger.info("Disconnecting from the server "
                    + socket.getRemoteSocketAddress());
            sendRelease();
            toServer.close();
            fromServer.close();
            socket.close();
        }
        catch (IOException e)
        {
            // It's OK if disconnect fails.
        }
    }

    /**
     * Filtering server protocol.
     */
    static class Protocol
    {
        public static final String VERSION           = "v0_9";

        /** Message types. */
        public static final String TYPE_PREPARE      = "prepare";
        public static final String TYPE_FILTER       = "filter";
        public static final String TYPE_RELEASE      = "release";
        public static final String TYPE_ACKNOWLEDGED = "acknowledged";
        public static final String TYPE_FILTERED     = "filtered";

        /**
         * Protocol assumes that message header is a single-level JSON object
         * ending with a closing curly brace.
         * 
         * @return Returns header or left part (JSON) of the message, i.e. the
         *         one without payload.
         */
        public static String getHeader(String message)
        {
            return message.substring(0, message.indexOf('}') + 1);
        }

        /**
         * Protocol assumes that message header is a single-level JSON object
         * ending with a closing curly brace.
         * 
         * @param message
         * @return Returns payload or right part of the message.
         */
        public static String getPayload(String message)
        {
            return message
                    .substring(message.indexOf('}') + 1, message.length());
        }
    }

    /**
     * Generator of client messages.
     */
    static class ClientMessageGenerator
    {
        private String service;

        public ClientMessageGenerator(String service)
        {
            this.service = service;
        }

        public String getService()
        {
            return service;
        }

        public String prepare()
        {
            StringBuilder sb = new StringBuilder();

            sb.append("{");
            sb.append("\"protocol\":\"" + Protocol.VERSION + "\",");
            sb.append("\"type\":\"" + Protocol.TYPE_PREPARE + "\",");
            sb.append("\"service\":\"" + service + "\",");
            sb.append("\"payload\":0");
            sb.append("}");

            return sb.toString();
        }

        public String filter(String transformation, long seqno, long row,
                String schema, String table, String column, String payload)
        {
            StringBuilder sb = new StringBuilder();

            sb.append("{");
            sb.append("\"protocol\":\"" + Protocol.VERSION + "\",");
            sb.append("\"type\":\"" + Protocol.TYPE_FILTER + "\",");
            sb.append("\"transformation\":\"" + transformation + "\",");
            sb.append("\"service\":\"" + service + "\",");
            sb.append("\"seqno\":" + seqno + ",");
            sb.append("\"row\":" + row + ",");
            sb.append("\"schema\":\"" + schema + "\",");
            sb.append("\"table\":\"" + table + "\",");
            sb.append("\"column\":\"" + column + "\",");
            sb.append("\"fragment\":1,");
            sb.append("\"fragments\":1,");
            sb.append("\"payload\":" + payload.length() + "");
            sb.append("}");
            sb.append(payload);

            return sb.toString();
        }

        public String release()
        {
            StringBuilder sb = new StringBuilder();

            sb.append("{");
            sb.append("\"protocol\":\"" + Protocol.VERSION + "\",");
            sb.append("\"type\":\"" + Protocol.TYPE_RELEASE + "\",");
            sb.append("\"service\":\"" + service + "\",");
            sb.append("\"payload\":0");
            sb.append("}");

            return sb.toString();
        }
    }

    /**
     * Generator of server messages.
     */
    static class ServerMessageGenerator
    {
        public String filtered(String service, String transformation,
                int returnCode, long seqno, long row, String schema,
                String table, String column, String payload)
        {
            StringBuilder sb = new StringBuilder();

            sb.append("{");
            sb.append("\"protocol\":\"" + Protocol.VERSION + "\",");
            sb.append("\"type\":\"" + Protocol.TYPE_FILTERED + "\",");
            sb.append("\"transformation\":\"" + transformation + "\",");
            sb.append("\"return\":" + returnCode + ",");
            sb.append("\"service\":\"" + service + "\",");
            sb.append("\"seqno\":" + seqno + ",");
            sb.append("\"row\":" + row + ",");
            sb.append("\"schema\":\"" + schema + "\",");
            sb.append("\"table\":\"" + table + "\",");
            sb.append("\"column\":\"" + column + "\",");
            sb.append("\"fragment\":1,");
            sb.append("\"fragments\":1,");
            sb.append("\"payload\":" + payload.length() + "");
            sb.append("}");
            sb.append(payload);

            return sb.toString();
        }

        public String acknowledged(String service, int returnCode,
                String payload)
        {
            StringBuilder sb = new StringBuilder();

            sb.append("{");
            sb.append("\"protocol\":\"" + Protocol.VERSION + "\",");
            sb.append("\"type\":\"" + Protocol.TYPE_ACKNOWLEDGED + "\",");
            sb.append("\"return\":" + returnCode + ",");
            sb.append("\"service\":\"" + service + "\",");
            sb.append("\"payload\":" + payload.length() + "");
            sb.append("}");
            sb.append(payload);

            return sb.toString();
        }
    }
}
