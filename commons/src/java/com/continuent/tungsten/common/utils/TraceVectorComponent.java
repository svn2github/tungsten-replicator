
package com.continuent.tungsten.common.utils;

public enum TraceVectorComponent
{
    cluster(0), manager(1), replicator(2), connector(3), router(4), monitor(5);

    private final int id;

    TraceVectorComponent(int id)
    {
        this.id = id;
    }

    public int getId()
    {
        return this.id;
    }

}
