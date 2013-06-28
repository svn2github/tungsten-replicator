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
 * Initial developer(s): Linas Virbalas
 * Contributor(s): 
 */

package com.continuent.tungsten.common.utils;

import java.lang.management.ThreadInfo;

/**
 * Methods to help diagnosing and debugging current JAVA process.
 * 
 * @author <a href="mailto:linas.virbalas@continuent.com">Linas Virbalas</a>
 * @version 1.0
 */
public class DiagnosticWizard
{
    /**
     * Obsolete due to Java 1.7 limitations. Returned all threads' ThreadInfo
     * objects. Note that some array elements might contain null elements if
     * threads died while enumerating them.
     * 
     * @return All available ThreadInfo objects for this process.
     * @throws Exception
     */
    @Deprecated
    public static ThreadInfo[] getAllThreadInfos() throws Exception
    {
        ThreadInfo[] infos = new ThreadInfo[0];
        return infos;
    }

    /**
     * Dumps a list of threads with their stack traces into the log.
     */
    @Deprecated
    public static String dumpThreadStack() throws Exception
    {
        return "# Feature removed due to Java 1.7 limitations";
    }

    /**
     * The effect is the same as calling diag(null).
     * 
     * @return Diagnostic information without component specific details.
     */
    @Deprecated
    public static String diag() throws Exception
    {
        return diag(null);
    }

    /**
     * Obsolete. Dumped various debugging information (thread list, thread stack
     * trace, internal data structures, etc.) to the log.
     * 
     * @throws Exception
     * @param componentDiag Component specific diagnostic information returning
     *            callback which data is included in the return value of this
     *            method.
     */
    @Deprecated
    public static String diag(DiagnosticWizardPlugin componentDiag)
            throws Exception
    {
        return "diag output is obsolete and not supported anymore";
    }
}
