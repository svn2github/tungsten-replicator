/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2013-2014 Continuent Inc.
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

package com.continuent.tungsten.replicator.datasource;

import org.junit.Before;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.common.csv.CsvSpecification;
import com.continuent.tungsten.replicator.datasource.FileDataSource;

/**
 * Implements a test on file data source operations.
 */
public class FileDataSourceTest extends AbstractDataSourceTest
{
    /**
     * Set up properties used to configure the data source.
     */
    @Before
    public void setUp() throws Exception
    {
        // Create the data source definition.
        datasourceProps = new TungstenProperties();
        datasourceProps.setString("serviceName", "sqlcatalog");
        datasourceProps.setLong("channels", 10);
        datasourceProps.setString("directory", "fileCatalogTest");
        datasourceProps.setString("csv", CsvSpecification.class.getName());
        datasourceProps.setString("csv.fieldSeparator", "\t");
        datasourceProps.setBeanSupportEnabled(true);

        // Set the data source class.
        datasourceClass = FileDataSource.class.getName();
    }
}