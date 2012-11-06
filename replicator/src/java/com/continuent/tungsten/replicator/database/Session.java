/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2012 Continuent Inc.
 * Contact: tungsten@continuent.com
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

package com.continuent.tungsten.replicator.database;

/**
 * This class defines an active session on the DBMS server.
 */
public class Session
{
    private String login;
    private Object identifier;

    /**
     * Creates an empty session definition.
     */
    public Session()
    {
    }

    /**
     * Creates a new session with preset values.
     * 
     * @param login User name
     * @param identifier A DBMS-specific value that can be used to kill the
     *            session
     */
    public Session(String login, Object identifier)
    {
        this.login = login;
        this.identifier = identifier;
    }

    public String getLogin()
    {
        return login;
    }

    public void setLogin(String login)
    {
        this.login = login;
    }

    public Object getIdentifier()
    {
        return identifier;
    }

    public void setIdentifier(Object identifier)
    {
        this.identifier = identifier;
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#toString()
     */
    public String toString()
    {
        StringBuffer sb = new StringBuffer();
        sb.append(this.getClass().getSimpleName());
        sb.append(" identifier=").append(identifier);
        sb.append(" login=").append(login);
        return sb.toString();
    }
}