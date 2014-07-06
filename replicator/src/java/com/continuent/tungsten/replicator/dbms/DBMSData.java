/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2013 Continuent Inc.
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
 * Initial developer(s): Teemu Ollakka
 * Contributor(s): Stephane Giron
 */

package com.continuent.tungsten.replicator.dbms;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import com.continuent.tungsten.replicator.event.ReplOption;

/**
 * Implements the core class for row and SQL statement updates. All update types
 * derive from this class.
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public class DBMSData implements Serializable
{
    static final long          serialVersionUID = -1;

    protected List<ReplOption> options          = null;

    /**
     * Creates a new <code>DBMSData</code> object
     */
    public DBMSData()
    {

    }

    /**
     * Add an option value. This can create duplicates the option exists.
     */
    public void addOption(String name, String value)
    {
        if (options == null)
            options = new LinkedList<ReplOption>();
        options.add(new ReplOption(name, value));
    }

    /**
     * Set the value, adding a new option if it does not exist or changing the
     * existing value.
     */
    public void setOption(String name, String value)
    {
        removeOption(name);
        addOption(name, value);
    }

    /**
     * Remove an option and return its value if it exists.
     */
    public String removeOption(String name)
    {
        // Remove previous value, if any.
        if (options != null)
        {
            ReplOption existingOption = null;
            for (ReplOption replOption : options)
            {
                if (name.equals(replOption.getOptionName()))
                    existingOption = replOption;
            }
            if (existingOption != null)
            {
                options.remove(existingOption);
                return existingOption.getOptionValue();
            }
        }
        return null;
    }

    /**
     * Return all options.
     */
    public List<ReplOption> getOptions()
    {
        return options;
    }

    /**
     * Returns an option value or null if not found.
     * 
     * @param name Option name
     */
    public String getOption(String name)
    {
        if (options == null)
            return null;
        else
        {
            for (ReplOption replOption : options)
            {
                if (name.equals(replOption.getOptionName()))
                    return replOption.getOptionValue();
            }
            return null;
        }
    }
}
