/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2009 Continuent Inc.
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
 * Initial developer(s): Edward Archibald
 * Contributor(s): 
 */

package com.continuent.tungsten.commons.utils;

import java.util.TreeMap;

public class HelpMap extends TreeMap<String, HelpItem>
{
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    private String category;
    
    public HelpMap(String category)
    {
        this.category = category;
    }
    
    public void put(String command, String usage, String description)
    {
        this.put(command, new HelpItem(command, usage, description));
    }
    
    public HelpItem getItem(String command)
    {
        return this.getItem(command);
    }

    public String getCategory()
    {
        return category;
    }

    public void setCategory(String category)
    {
        this.category = category;
    }
}
