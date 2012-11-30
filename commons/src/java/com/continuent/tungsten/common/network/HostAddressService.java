/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2011-2012 Continuent Inc.
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

package com.continuent.tungsten.common.network;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.log4j.Logger;

/**
 * Implements a service for performing operations on Internet addresses, such as
 * testing liveness. This class is designed to make calls to ping hosts as
 * robust and as simple as possible, at the cost of a little more up-front
 * configuration in some cases, for example to set timeouts.
 * <p/>
 * This class is thread-safe through the use of synchronized methods to access
 * the method table and enabled names list. The timeout is volatile, which
 * obviates the need for synchronization.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class HostAddressService
{
    /** Logger for this class */
    private static final Logger           logger        = Logger.getLogger(HostAddressService.class);

    /** Default Java ping method using InetAddress.isReachable(). */
    public static String                  DEFAULT       = "default";

    /** Ping method using operating system ping command. */
    public static String                  PING          = "ping";

    // Ping methods are stored in a list as well as a hash index. The names list
    // contains only enabled methods. Access to these *must* be synchronized to
    // preserve thread safety.
    private List<String>                  names         = new LinkedList<String>();
    private ConcurrentMap<String, String> methods       = new ConcurrentHashMap<String, String>();

    // Timeout for ping operations in milliseconds.
    private volatile int                  timeoutMillis = 5000;

    /**
     * Creates a new service.
     * 
     * @param autoEnable If true, enable ping methods automatically.
     * @throws HostException Thrown if there is a problem enabling a method
     */
    public HostAddressService(boolean autoEnable) throws HostException
    {
        // Add known ping methods.
        addMethod(DEFAULT, InetAddressPing.class.getName(), autoEnable);
        addMethod(PING, OsUtilityPing.class.getName(), autoEnable);
    }

    /**
     * Sets the timeout for ping methods. Methods will try for up to this time
     * before giving up.
     * 
     * @param timeoutMillis Timeout in milliseconds
     */
    public void setTimeout(int timeoutMillis)
    {
        this.timeoutMillis = timeoutMillis;
    }

    /**
     * Returns current timeout in milliseconds.
     */
    public int getTimeout()
    {
        return timeoutMillis;
    }

    /**
     * Adds a ping method to the service.
     * 
     * @param name Logical name of the method
     * @param methodClass Method class name
     * @param enable If true, enable the method for use
     * @throws HostException Thrown if there is a problem enabling a method.
     */
    public synchronized void addMethod(String name, String methodClass,
            boolean enable) throws HostException
    {
        if (logger.isDebugEnabled())
        {
            logger.debug("Adding ping method: name=" + name + " class="
                    + methodClass);
        }

        // Ensure that we can instantiate a ping method. This is a minimal
        // check to ensure the ping method will succeed.
        instantiatePingMethod(methodClass);

        // Add to table of available methods and optionally enable.
        methods.put(name, methodClass);
        if (enable)
            enableMethod(name);
    }

    /**
     * Enables a ping method.
     * 
     * @param name of method to enable
     * @throws HostException Thrown if method name does not exist
     */
    public synchronized void enableMethod(String name) throws HostException
    {
        String methodClass = methods.get(name);
        if (methodClass == null)
        {
            StringBuffer sb = new StringBuffer();
            for (String legalName : this.getAvailableMethodNames())
            {
                if (sb.length() > 0)
                    sb.append(",");
                sb.append(legalName);
            }
            throw new HostException(String.format(
                    "Unknown ping method name; legal values are (%s): %s",
                    sb.toString(), name));
        }
        else
        {
            if (!names.contains(name))
                names.add(name);
        }
    }

    /**
     * Returns names of available ping methods, whether enabled or not.
     */
    public synchronized List<String> getAvailableMethodNames()
    {
        Set<String> allNames = this.methods.keySet();
        return new ArrayList<String>(allNames);
    }

    /**
     * Returns names of available ping methods.
     */
    public synchronized List<String> getEnabledMethodNames()
    {
        return names;
    }

    /**
     * Returns a ping method by name or null if no such method exists.
     */
    public synchronized String getMethodName(String name)
    {
        return methods.get(name);
    }

    /** Returns a host address instance. */
    public HostAddress getByName(String host) throws UnknownHostException
    {
        InetAddress inetAddress = InetAddress.getByName(host);
        HostAddress address = new HostAddress(inetAddress);
        return address;
    }

    /**
     * Returns true if the host is reachable by an available ping method. This
     * method clears previous notifications.
     * 
     * @param host Name of host for which we want to test reachability
     * @return True if host is reachable, otherwise false
     * @throws HostException Thrown if a ping method fails
     */
    public PingResponse isReachable(HostAddress host) throws HostException
    {

        // Compose a response.
        PingResponse response = new PingResponse();
        response.setReachable(false);

        // Try all methods.
        for (String name : this.getEnabledMethodNames())
        {
            PingNotification notification = _isReachableByMethod(name, host);
            response.addNotification(notification);
            response.setReachable(notification.isReachable());

            if (response.isReachable())
            {
                break;
            }
        }

        // Return the response;
        return response;
    }

    /**
     * Returns true if the host is reachable by an available ping method. This
     * method clears previous notifications.
     * 
     * @param name Name of ping method to use
     * @param host Name of host for which we want to test reachability
     * @return True if host is reachable, otherwise false
     * @throws HostException Thrown if a ping method fails
     */
    public PingResponse isReachableByMethod(String name, HostAddress host)
            throws HostException
    {
        PingResponse response = new PingResponse();
        PingNotification notification = _isReachableByMethod(name, host);
        response.addNotification(notification);
        response.setReachable(notification.isReachable());
        return response;
    }

    // Private method to check reachability without clearning notifications.
    public PingNotification _isReachableByMethod(String name, HostAddress host)
            throws HostException
    {
        if (logger.isDebugEnabled())
        {
            logger.debug("Testing host reachability: method=" + name + " host="
                    + host.toString() + " timeout=" + timeoutMillis);
        }
        String methodClass = getMethodName(name);
        if (name == null)
        {
            throw new HostException("Unknown ping method: " + name);
        }
        else
        {
            // Set up the notification to be used in the response.
            PingNotification notification = new PingNotification();
            notification.setHostName(host.getCanonicalHostName());
            notification.setMethodName(name);
            notification.setTimeout(timeoutMillis);

            long startMillis = System.currentTimeMillis();
            PingMethod method = null;
            try
            {
                // Instantiate the ping method and prepare it for use.
                method = instantiatePingMethod(methodClass);

                // Make the call.
                boolean status = method.ping(host, timeoutMillis);

                // Fill in missing ping information.
                notification.setReachable(status);
            }
            catch (Exception e)
            {
                // Fill in notification information for an exception.
                notification.setReachable(false);
                notification.setException(e);
            }
            finally
            {
                long duration = System.currentTimeMillis() - startMillis;
                notification.setDuration(duration);
                notification.setNotes(method.getNotes());
            }

            // Return the completed notification.
            return notification;
        }
    }

    // Instantiates and returns a ping method instance.
    private PingMethod instantiatePingMethod(String methodClass)
            throws HostException
    {
        try
        {
            PingMethod method = (PingMethod) Class.forName(methodClass)
                    .newInstance();
            return method;
        }
        catch (Throwable e)
        {
            String msg = String
                    .format("Unexpected failure while instantiating ping method: name=%s class=%s",
                            methodClass, methodClass);
            throw new HostException(msg, e);
        }

    }
}