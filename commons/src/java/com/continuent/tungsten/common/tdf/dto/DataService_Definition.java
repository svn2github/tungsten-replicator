/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2009 Continuent Inc.
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
 * Initial developer(s): Ludovic Launer
 * Contributor(s):
 */

package com.continuent.tungsten.common.tdf.dto;

import java.util.HashMap;

import javax.xml.bind.annotation.XmlRootElement;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonPropertyOrder;
import org.codehaus.jackson.map.annotate.JsonSerialize;

/**
 * Data Transfer Object for: Data Service. Definition used when creating a new
 * resource
 * 
 * @author <a href="mailto:ludovic.launer@continuent.com">Ludovic Launer</a>
 * @version 1.0
 */
@XmlRootElement
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonPropertyOrder({"name"})
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class DataService_Definition extends Service
{
    // --- Data Service definition ---
    private HashMap<String, DataSource_Definition> listDataSourceDefinition = new HashMap<String, DataSource_Definition>();

    public DataService_Definition()
    {
    }

    public DataService_Definition(Service parentService, String name)
    {
        super(parentService);
        this.name = name;

    }
    
    /**
     * Add a DataSource to the DataService
     * 
     * @param dataSourceDefinition
     * @return
     */
    public HashMap<String, DataSource_Definition> addDataSource(DataSource_Definition dataSourceDefinition)
    {
        this.listDataSourceDefinition.put(dataSourceDefinition.getName(), dataSourceDefinition);
        return this.listDataSourceDefinition;
    }

    // --------------------------------- Getters and Setters ------------------
    // @formatter:off
	public HashMap<String, DataSource_Definition> getListDataSourceDefinition() 								{return this.listDataSourceDefinition;}
	public void setListDataSourceDefinition(HashMap<String, DataSource_Definition> listDataSourceDefinition) 	{this.listDataSourceDefinition = listDataSourceDefinition;}
	// @formatter:on

}
