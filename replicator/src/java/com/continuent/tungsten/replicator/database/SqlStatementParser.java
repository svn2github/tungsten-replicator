/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2013 Continuent Inc.
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

package com.continuent.tungsten.replicator.database;

import java.util.HashMap;

/**
 * Handles parsing of SQL statements to derive parsing information. This class
 * encapsulates logic to select a parser based on a particular dialect.
 * <p/>
 * The call to fetch the singleton parser is synchronized to guarantee
 * visibility across all threads. Also, we final variables for all instances to
 * avoid unnecessary object creation, as the parsing function is called
 * potentially many times.
 */
public class SqlStatementParser
{
    // Singleton parser.
    private static SqlStatementParser                  parser       = new SqlStatementParser();

    // Map of available operation matchers.
    private final HashMap<String, SqlOperationMatcher> matchers     = new HashMap<String, SqlOperationMatcher>();
    // Singleton matcher for MySQL to eliminate unnecessary object creation.
    private final SqlOperationMatcher                  mysqlMatcher = new MySQLOperationMatcher();

    /** Instantiates a SqlStatementParser and loads the map. */
    private SqlStatementParser()
    {
        matchers.put(Database.MYSQL, mysqlMatcher);

        // This is lame but we only support MySQL statement parsing at this
        // point.
        matchers.put(Database.ORACLE, mysqlMatcher);
        matchers.put(Database.POSTGRESQL, mysqlMatcher);
        matchers.put(Database.UNKNOWN, mysqlMatcher);
    }

    /**
     * Returns a SQL statement parser.
     */
    public static synchronized SqlStatementParser getParser()
    {
        return parser;
    }

    /**
     * Parse a SQL statement.
     * 
     * @param statement A query, presumably written in some form of SQL
     * @param dbmsType The DBMS type, using one of the string names provided by
     *            the Database class.
     * @return A SqlOperation containing parsing metadata
     */
    public SqlOperation parse(String statement, String dbmsType)
    {
        SqlOperationMatcher matcher = matchers.get(dbmsType);
        if (matcher == null)
            matcher = new MySQLOperationMatcher();
        return matcher.match(statement);
    }
}