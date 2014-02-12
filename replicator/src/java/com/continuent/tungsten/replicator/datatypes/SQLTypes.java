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

package com.continuent.tungsten.replicator.datatypes;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

/**
 * Helper class to transform java.sql.Types integer values into readable strings.
 * 
 * @author <a href="mailto:linas.virbalas@continuent.com">Linas Virbalas</a>
 * @version 1.0
 */
public class SQLTypes
{
    private Map<Integer, String> sqlTypes = new HashMap<Integer, String>();

    public SQLTypes()
    {
        // Initialize available SQL types.
        for (Field field : java.sql.Types.class.getFields())
        {
            try
            {
                sqlTypes.put((Integer) field.get(null), field.getName());
            }
            catch (IllegalArgumentException e)
            {
                // Ignore.
            }
            catch (IllegalAccessException e)
            {
                // Ignore.
            }
        }
    }

    /**
     * Transforms integer, representing java.sql.Types value, into to human
     * readable string.
     * 
     * @param sqlType java.sql.Types value.
     * @return String representation (name) of the java.sql.Types variable.
     *         null, if translation was not found.
     */
    public String sqlTypeToString(int sqlType)
    {
        return sqlTypes.get(sqlType);
    }
}
