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
 * Initial developer(s): Robert Hodges
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.thl;

import java.util.List;

/**
 * Implements a simple strategy class that hands out connection URIs and
 * allows the caller to cycle through a list of them.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class ConnectUriManager
{
    private int          index = 0;
    private List<String> uriList;

    /**
     * Creates a new instance with a list of URIs.
     * 
     * @param connectUri List of one or more URIs
     * @throws THLException Thrown if array is 0 length
     */
    public ConnectUriManager(List<String> connectUri) throws THLException
    {
        this.uriList = connectUri;
        if (connectUri.size() == 0)
        {
            throw new THLException(
                    "Connect URI value is empty; must be a list of one or more THL URIs");
        }
    }

    /**
     * Returns the next THL URI in the list.
     */
    public String next()
    {
        if (index >= uriList.size())
            index = 0;
        return uriList.get(index++);
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#toString()
     */
    public String toString()
    {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < uriList.size(); i++)
        {
            if (i > 0)
                sb.append(",");
            sb.append(uriList.get(i));
        }
        return sb.toString();
    }
}