
package com.continuent.tungsten.common.utils;

public enum CLLogLevel
{
    off(0), summary(1), normal(2), detailed(3), debug(4), diag(5);

    private final int level;

    CLLogLevel(int level)
    {
        this.level = level;
    }

    public int getLevel()
    {
        return this.level;
    }

}
