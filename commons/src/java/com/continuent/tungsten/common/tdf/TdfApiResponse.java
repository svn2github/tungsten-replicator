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

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

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
   
    protected URI     requestURI         = null;    /** The initial request URI as sent to the API */
    protected Object  inputObject        = null;    /** The input object passed as a parameter to the API */
    protected URI     responseURI        = null;    /** Return message related to the returnCode and adding more information */
    protected Integer returnCode         = null;    /** The response Object providing the API return value */ 
    protected String  returnMessage      = null;    /** The type of the outputPayload Object */
    protected Object  outputPayload      = null;

    private Class<?>  outputPayloadClass = null;

    /**
     * Creates a new <code>TdfApiResponse</code> object
     */
    public TdfApiResponse()
    {
    }

    /**
     * Creates a new <code>TdfApiResponse</code> object
     * 
     * @param requestURI The URI corresponding to the submitted request.
     */
    public TdfApiResponse(URI requestURI)
    {
        this.requestURI = requestURI;
    }

    /**
     * Creates a new <code>TdfApiResponse</code> object.
     * private constructor to enforce object creation through builder
     * @param builder
     */
    private TdfApiResponse(Builder builder)
    {
        this.requestURI = builder.requestURI;
        this.inputObject = builder.inputObject;
        this.responseURI = builder.responseURI;
        this.returnCode = builder.returnCode;
        this.returnMessage = builder.returnMessage;
        this.outputPayload = builder.outputPayload;
    }

    /**
     * Returns the outputPayloadClass value.
     * 
     * @return Returns the outputPayloadClass.
     */
    public Class<?> getOutputPayloadClass()
    {
        this.outputPayloadClass = (this.outputPayload == null)
                ? null
                : outputPayload.getClass();
        return outputPayloadClass;
    }
    
    /**
     * Get the returnMessage.
     * If null, tries to get the return Message from the returnCode.
     * 
     * @return the returnMessage as set by the user, or as derived from the returnCode
     */
    public String getReturnMessage()
    {
        if (this.returnMessage==null && this.returnCode!=null)
        {
            Status status = Response.Status.fromStatusCode(this.returnCode);
            this.returnMessage = (status!=null)? status.getReasonPhrase() : null;
        }
            
        return this.returnMessage;
    }
    
   
    public URI getRequestURI()                          {return requestURI;}
    public void setRequestURI(URI requestURI)           {this.requestURI = requestURI; }

    public Object getInputObject()                      {return inputObject;}
    public void setInputObject(Object inputObject)      {this.inputObject = inputObject;}

    public URI getResponseURI()                         {return responseURI;}
    public void setResponseURI(URI responseURI)         {this.responseURI = responseURI;}

    public Integer getReturnCode()                      {return returnCode;}
    public void setReturnCode(Integer returnCode)       {this.returnCode = returnCode;}

//    public String getReturnMessage()                    {return returnMessage;}
    public void setReturnMessage(String returnMessage)  {this.returnMessage = returnMessage;}

    public Object getOutputPayload()                    {return outputPayload;}
    public void setOutputPayload(Object outputPayload)  {this.outputPayload = outputPayload;}

    // ################################################################################ Builder for TdfApiResponse #################################################################
    /**
     * Builder class for a TdfApiResponse
     * 
     * @author <a href="mailto:ludovic.launer@continuent.com">Ludovic Launer</a>
     * @version 1.0
     */
    public static class Builder
    {

        private URI     requestURI    = null;
        private Object  inputObject   = null;
        private URI     responseURI   = null;
        private Integer returnCode    = null;
        private String  returnMessage = null;
        private Object  outputPayload = null;

        // builder methods for setting property
        public Builder requestURI(      URI     requestURI)     {this.requestURI    = requestURI;       return this;}
        public Builder inputObject(     Object  inputObject)    {this.inputObject   = inputObject;      return this;}
        public Builder responseURI(     URI     responseURI)    {this.responseURI   = responseURI;      return this;}
        public Builder returnCode(      Integer returnCode)     {this.returnCode    = returnCode;       return this;}
        public Builder returnMessage(   String  returnMessage)  {this.returnMessage = returnMessage;    return this;}
        public Builder outputPayload(   Object  outputPayload)  {this.outputPayload = outputPayload;    return this;}

       
        /**
         * Build a TdfApiResponse
         * 
         * @return fully built object
         */
        public TdfApiResponse build()
        {
            return new TdfApiResponse(this);
        }
        
    }

}
