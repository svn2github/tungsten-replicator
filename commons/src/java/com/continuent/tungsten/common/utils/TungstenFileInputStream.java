/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2012 Continuent Inc.
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
 */

package com.continuent.tungsten.common.utils;

/**
 * This class 'wraps' FileInputStream and gives us the ability
 * to work around an issue in the explicit finalization provided in that class.
 * 
 * WARNING:  This class should only be used in places where the FileInputStream
 * is opened and then immediately closed.
 */

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

public class TungstenFileInputStream extends FileInputStream
{
    private final boolean debug = false;

    public TungstenFileInputStream(File file) throws FileNotFoundException
    {
        super(file);
    }

    protected void finalize() throws IOException
    {
        if (debug)
        {
            System.out.println(String.format("FINALIZE: %s : getFD()=%s", this
                    .toString(),
                    (getFD() == null ? "null" : getFD().toString())));
        }
        try
        {
            if (getFD() != null)
            {
                super.close();
            }
        }
        finally
        {

        }
    }
}
