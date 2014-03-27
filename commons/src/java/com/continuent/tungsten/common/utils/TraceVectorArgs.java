
package com.continuent.tungsten.common.utils;

public class TraceVectorArgs
{
    private TraceVectorComponent component;
    private String               category = null;
    private int                  target   = -1;

    public TraceVectorArgs(TraceVectorComponent component, String category,
            int target)
    {
        this.component = component;
        this.category = category;
        this.target = target;
    }

    static TraceVectorArgs getArgs(String vectorPath) throws Exception
    {
        String pathElements[] = vectorPath.split("/");

        if (pathElements.length != 3)
        {
            throw new Exception(String.format("malformed vector path '%s'",
                    vectorPath));
        }

        TraceVectorComponent component = TraceVectorComponent
                .valueOf(pathElements[0]);
        String category = pathElements[1];
        int target = Integer.parseInt(pathElements[2]);

        return new TraceVectorArgs(component, category, target);
    }

    public TraceVectorComponent getComponent()
    {
        return component;
    }

    public void setComponent(TraceVectorComponent component)
    {
        this.component = component;
    }

    public String getCategory()
    {
        return category;
    }

    public void setCategory(String category)
    {
        this.category = category;
    }

    public int getTarget()
    {
        return target;
    }

    public void setTarget(int target)
    {
        this.target = target;
    }

    /**
     * Provides a path representation of this instance
     * 
     * @return the string "<component>/<category>/<target>"
     */
    public String toVectorPath()
    {
        return toVectorPath(component, category, target);
    }

    /**
     * Provides a path representation of the given trace vector elements
     * 
     * @return the string "<component>/<category>/<target>"
     */
    public static String toVectorPath(TraceVectorComponent component,
            String category, int target)
    {
        return String
                .format("%s/%s/%d", component.toString(), category, target);
    }
}
