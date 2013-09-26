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

import java.net.URI;

import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import javax.xml.bind.annotation.XmlRootElement;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonPropertyOrder;
import org.codehaus.jackson.map.annotate.JsonSerialize;

/**
 * Class describing a metadata embeded in a Response
 * 
 * @author <a href="mailto:ludovic.launer@continuent.com">Ludovic Launer</a>
 * @version 1.0
 */

@XmlRootElement
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonPropertyOrder(alphabetic = true)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class TdfResourceLocation
{
    public URI statusUrl                 = null;
    public URI location                  = null;
    public URI commandUrl                = null;
    public URI listDescriptionServiceUrl = null;

    public TdfResourceLocation()
    {

    }

    /**
     * Creates a new <code>TdfResponseMetadata</code> object
     * 
     * @param uriInfo
     * @param service
     */
    @SuppressWarnings("rawtypes")
    public TdfResourceLocation(UriInfo uriInfo, Service service,
            Class statusResource, Class serviceResource, Class commandResource)
    {
        String parentPath = this.getParentPath(service).toString();
        parentPath = (parentPath == null) ? "." : parentPath;

        // --- Build absolute URLs ---
        this.statusUrl = UriBuilder.fromUri(uriInfo.getBaseUri())
                .path(statusResource).path(parentPath).path(service.getName())
                .build();
        this.location = UriBuilder.fromUri(uriInfo.getBaseUri())
                .path(serviceResource).path(parentPath).path(service.getName())
                .build();
        this.commandUrl = UriBuilder.fromUri(uriInfo.getBaseUri())
                .path(commandResource).path(parentPath).path(service.getName())
                .build();

        // --- Turn then into relative URLs ---
        this.statusUrl = UriBuilder.fromPath(this.statusUrl.getPath()).build();
        this.location = UriBuilder.fromPath(this.location.getPath()).build();
        this.commandUrl = UriBuilder.fromPath(this.commandUrl.getPath())
                .build();
    }

    /**
     * Generate path corresponding to the suffix for a Service
     * 
     * @param service
     * @return path to be prepended to the current service path
     */
    private URI getParentPath(Service service)
    {
        Service parent = service.getParentService();
        URI parentPath = UriBuilder.fromPath("").build();

        while (parent != null)
        {
            // If it's a single site service, do not preprend the name of the
            // service to the name of the DataService = it's the same
            if (parent instanceof Service_Definition
                    && !((Service_Definition) parent).isCompositeService())
                break;
            parentPath = UriBuilder.fromPath(parent.getName())
                    .path(parentPath.toString()).build();
            parent = parent.getParentService();
        }

        return parentPath;

    }

}
