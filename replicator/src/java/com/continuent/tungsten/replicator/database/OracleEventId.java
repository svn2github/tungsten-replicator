/**
 * 
 */

package com.continuent.tungsten.replicator.database;

import java.util.Scanner;

/**
 * This class defines a OracleEventId
 * 
 * @author <a href="mailto:jussi-pekka.kurikka@continuent.com">Jussi-Pekka
 *         Kurikka</a>
 * @version 1.0
 */
public class OracleEventId implements EventId
{
    private long    scn = -1;
    private boolean valid;

    public OracleEventId(String rawEventId)
    {
        String eventId = rawEventId.substring(4).trim();
        Scanner scan = new Scanner(eventId);
        if (scan.hasNextLong())
            scn = scan.nextLong();
        else
            valid = false;
        if (scan.hasNext())
            valid = false;
        else
            valid = true;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.EventId#getDbmsType()
     */
    @Override
    public String getDbmsType()
    {
        return "oracle";
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.EventId#isValid()
     */
    @Override
    public boolean isValid()
    {
        return valid;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.database.EventId#compareTo(com.continuent.tungsten.replicator.database.EventId)
     */
    @Override
    public int compareTo(EventId eventId)
    {
        OracleEventId event = (OracleEventId) eventId;
        long l = event.getSCN() - this.getSCN();
        return (l == 0 ? 0 : (l > 0 ? -1 : 1));
    }

    public long getSCN()
    {
        return scn;
    }

}
