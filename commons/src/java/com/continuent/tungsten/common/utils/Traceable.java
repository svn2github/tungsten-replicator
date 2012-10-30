
package com.continuent.tungsten.common.utils;

public interface Traceable
{
    public String trace(String vectorPath, boolean enable) throws Exception;

    public String traceReset();

    public String traceEnable(boolean enable);

    public String traceAutoDisable(boolean enable);

    public String traceList();

}
