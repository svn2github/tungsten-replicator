/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2012 Continuent Inc.
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

package com.continuent.tungsten.commons.config.test;

/**
 * This class is used to test setting values on an object that contains embedded
 * Java beans.
 */
public class SampleContainingObject
{
    private String       myString;
    private SampleObject myObject1;
    private SampleObject myObject2;

    public SampleContainingObject()
    {
    }

    public String getMyString()
    {
        return myString;
    }

    public void setMyString(String string)
    {
        this.myString = string;
    }

    public SampleObject getMyObject1()
    {
        return myObject1;
    }

    public void setMyObject1(SampleObject myObject1)
    {
        this.myObject1 = myObject1;
    }

    public SampleObject getMyObject2()
    {
        return myObject2;
    }

    public void setMyObject2(SampleObject myObject2)
    {
        this.myObject2 = myObject2;
    }

    public boolean equals(Object o)
    {
        if (o == null)
            return false;
        if (!(o instanceof SampleContainingObject))
            return false;

        SampleContainingObject to = (SampleContainingObject) o;

        if (!compare(myString, to.getMyString()))
            return false;
        if (!compare(myObject1, to.getMyObject1()))
            return false;
        if (!compare(myObject2, to.getMyObject2()))
            return false;

        return true;
    }

    // Object comparison helper method.
    private boolean compare(Object o1, Object o2)
    {
        if (o1 == null)
            return (o2 == null);
        else if (!o1.getClass().equals(o2.getClass()))
            return false;
        else if (!o1.equals(o2))
            return false;
        else
            return true;
    }

    public enum SampleEnum
    {
        ONE, TWO, THREE
    }
}