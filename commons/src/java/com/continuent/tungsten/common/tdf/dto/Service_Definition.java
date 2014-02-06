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
 * Data Transfer Model for: Service. The Service_Definition class is tha root
 * parent class for all services : DataService_Definition,
 * CompositeDataService_Definition, ...
 * 
 * @author <a href="mailto:ludovic.launer@continuent.com">Ludovic Launer</a>
 * @version 1.0
 */

@XmlRootElement
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonPropertyOrder({"name", "compositeService"})
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class Service_Definition extends Service
{
    // --- Service Definition ---
    /** List of Sites = Data Services in the Service */
    private HashMap<String, DataService_Definition> listDataServiceDefinition = new HashMap<String, DataService_Definition>();

    public Service_Definition()
    {
    }

    /**
     * Creates a new <code>Service_Definition</code> object
     * 
     * @param name
     */
    public Service_Definition(String name)
    {
        super(name, null);
    }

    /**
     * Multi site = Composite Data Service. Single site = Data Service
     * 
     * @return True if the service is a multi site service. False if it's a
     *         single site service.
     */
    public boolean isCompositeService()
    {
        boolean isComposite = false;

        // Get info from reported list of Service
        if (this.listDataServiceDefinition != null
                && this.listDataServiceDefinition.size() > 1)
            isComposite = true;
        else
            isComposite = false;

        return isComposite;
    }

    /**
     * Add a DataService to the Service
     * 
     * @param dataServiceDefinition
     * @return HashMap<String, DataSource_Definition> the current list of
     *         DataSerivce_Definition
     */
    public HashMap<String, DataService_Definition> addDataService(
            DataService_Definition dataServiceDefinition)
    {
        this.listDataServiceDefinition.put(dataServiceDefinition.getName(),
                dataServiceDefinition); // Add a DataService

        return this.listDataServiceDefinition;
    }

    // --------------------------------- Getters and Setters ------------------
    @Override
    public String getName()
    {
        // By default Single site service have the name of their unique
        // DataService
        if (this.listDataServiceDefinition.size() == 1)
        {
            String dataServiceName = this.listDataServiceDefinition.keySet().iterator().next();
            this.name = dataServiceName;
        }
        return this.name;
    }
    
    /**
     * Sets the name of the Service.
     * Used for Composite DataService only
     */
    public HashMap<String, DataService_Definition> getListDataServiceDefinition()
    {
        return this.listDataServiceDefinition;
    }

    public void setListDataServiceDefinition(
            HashMap<String, DataService_Definition> listDataServiceDefinition)
    {
        this.listDataServiceDefinition = listDataServiceDefinition;
    }

    // ########################################################################

}
