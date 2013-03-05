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
 * Initial developer(s): Ludovic Launer
 */

package com.continuent.tungsten.common.tdf;

import java.net.URI;

import org.codehaus.jackson.map.annotate.JsonSerialize;

/**
 * This class defines a APIResponse. Used by the manager API and TDF API as
 * response class to all of the calls.
 * 
 * @author <a href="mailto:ludovic.launer@continuent.com">Ludovic Launer</a>
 * @version 1.0
 */
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class TdfApiResponse
{

    /** The initial request URI as sent to the API */
    protected URI requestURI  = null;
    /** The input object passed as a parameter to the API */
    protected Object      inputObject = null;

    protected URI responseURI = null;
    /**
     * The HTTP return code
     * @see <a href="http://en.wikipedia.org/wiki/List_of_HTTP_status_codes">HTTP return codes</a>
     */
    protected Integer     returnCode  = null;
    /** Return message related to the returnCode and adding more information*/
    protected String returnMessage = null;
    /** The response Object providing the API return value*/
    protected Object outputPayload = null;
    /** The type of the outputPayload Object*/
    private Class<?> outputPayloadClass= null;
    
    
    /**
     * Returns the outputPayloadClass value.
     * 
     * @return Returns the outputPayloadClass.
     */
    public Class<?> getOutputPayloadClass()
    {
        this.outputPayloadClass = (this.outputPayload==null)? null : outputPayload.getClass();
        return outputPayloadClass;
    }
    
    /**
     * Returns the requestURI value.
     * 
     * @return Returns the requestURI.
     */
    public URI getRequestURI()
    {
        return requestURI;
    }
    /**
     * Sets the requestURI value.
     * 
     * @param requestURI The requestURI to set.
     */
    public void setRequestURI(URI requestURI)
    {
        this.requestURI = requestURI;
    }
    /**
     * Returns the inputObject value.
     * 
     * @return Returns the inputObject.
     */
    public Object getInputObject()
    {
        return inputObject;
    }
    /**
     * Sets the inputObject value.
     * 
     * @param inputObject The inputObject to set.
     */
    public void setInputObject(Object inputObject)
    {
        this.inputObject = inputObject;
    }
    /**
     * Returns the responseURI value.
     * 
     * @return Returns the responseURI.
     */
    public URI getResponseURI()
    {
        return responseURI;
    }
    /**
     * Sets the responseURI value.
     * 
     * @param responseURI The responseURI to set.
     */
    public void setResponseURI(URI responseURI)
    {
        this.responseURI = responseURI;
    }
    /**
     * Returns the returnCode value.
     * 
     * @return Returns the returnCode.
     */
    public Integer getReturnCode()
    {
        return returnCode;
    }
    /**
     * Sets the returnCode value.
     * 
     * @param returnCode The returnCode to set.
     */
    public void setReturnCode(Integer returnCode)
    {
        this.returnCode = returnCode;
    }
    /**
     * Returns the returnMessage value.
     * 
     * @return Returns the returnMessage.
     */
    public String getReturnMessage()
    {
        return returnMessage;
    }
    /**
     * Sets the returnMessage value.
     * 
     * @param returnMessage The returnMessage to set.
     */
    public void setReturnMessage(String returnMessage)
    {
        this.returnMessage = returnMessage;
    }
    /**
     * Returns the outputPayload value.
     * 
     * @return Returns the outputPayload.
     */
    public Object getOutputPayload()
    {
        return outputPayload;
    }
    /**
     * Sets the outputPayload value.
     * 
     * @param outputPayload The outputPayload to set.
     */
    public void setOutputPayload(Object outputPayload)
    {
        this.outputPayload = outputPayload;
    }
    
    
    
    

}
