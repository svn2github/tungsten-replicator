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
 * Initial developer(s): Stephane Giron
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.database;

import java.util.Scanner;

/**
 * This class defines a OracleEventId
 * 
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class OracleEventId implements EventId
{
    private long    scn = -1;
    private boolean valid;

    public OracleEventId(String rawEventId)
    {
        String eventId;
        if (rawEventId.startsWith("ora:"))
            eventId = rawEventId.substring(4).trim();
        else
            eventId = rawEventId;

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

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        return "ora:" + String.valueOf(scn);
    }

}
