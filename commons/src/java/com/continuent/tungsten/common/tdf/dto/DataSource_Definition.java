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

import javax.xml.bind.annotation.XmlRootElement;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonPropertyOrder;
import org.codehaus.jackson.map.annotate.JsonSerialize;

/**
 * Data Transfer Object for: Server Definition = ServerConf. Definition used
 * when creating a new resource
 * 
 * @author <a href="mailto:ludovic.launer@continuent.com">Ludovic Launer</a>
 * @version 1.0
 */

@XmlRootElement
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonPropertyOrder(alphabetic = true)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class DataSource_Definition extends Service
{
    public String hostname = null;

    public DataSource_Definition()
    {
    }

    /**
     * Creates a new <code>Server_Definition</code> object
     * 
     * @param parentService
     * @param name
     * @param hostname
     */
    public DataSource_Definition(Service parentService, String name,
            String hostname)
    {
        super(parentService);

        this.name = name;
        this.hostname = hostname;
    }

}
