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
 * This class defines a database user.
 */
public class User
{
    private String  login;
    private String  password;
    private boolean privileged;

    /**
     * Creates an empty user definition.
     */
    public User()
    {
    }

    /**
     * Creates a new user with preset values.
     * 
     * @param login User login
     * @param password Password for account
     * @param privileged If true, this is a superuser login
     */
    public User(String login, String password, boolean privileged)
    {
        this.login = login;
        this.password = password;
        this.privileged = privileged;
    }

    public String getLogin()
    {
        return login;
    }

    public void setLogin(String login)
    {
        this.login = login;
    }

    public String getPassword()
    {
        return password;
    }

    public void setPassword(String password)
    {
        this.password = password;
    }

    public boolean isPrivileged()
    {
        return privileged;
    }

    public void setPrivileged(boolean privileged)
    {
        this.privileged = privileged;
    }
}
