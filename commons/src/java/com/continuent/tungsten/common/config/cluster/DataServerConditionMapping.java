
package com.continuent.tungsten.common.config.cluster;

import java.io.Serializable;

import com.continuent.tungsten.common.cluster.resource.ResourceState;
import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.common.mysql.MySQLIOs.ExecuteQueryStatus;

/**
 * This class holds the mapping for a given data server condition, as generated
 * in the monitoring subsystem, to a given ResourceState, action and threshold.
 * <p>
 * The ResourceState represents a very coarse categorization of the condition.
 * </p>
 * <p>
 * The 'action' represents the type of action the rules engine should take when
 * the threshold is exceeded.
 * </p>
 * <p>
 * The 'threshold' is the number of times the condition should be re-tested
 * before taking the specified action. Thresholds are specified in increments of
 * 10 seconds.
 * </p>
 * 
 * @author <a href="mailto:edward.archibald@continuent.com">Edward Archibald</a>
 * @version 1.0
 */
public class DataServerConditionMapping implements Serializable
{
    /**
     * 
     */
    private static final long  serialVersionUID = 1L;
    public static final String STATE_KEY        = "state";
    public static final String ACTION_KEY       = "action";
    public static final String THRESHOLD_KEY    = "threshold";

    private ExecuteQueryStatus queryStatus      = ExecuteQueryStatus.UNDEFINED;
    private ResourceState      state            = ResourceState.UNKNOWN;
    private MappedAction       action           = MappedAction.UNDEFINED;
    private int                threshold        = -1;

    public DataServerConditionMapping()
    {

    }

    /**
     * Creates a new <code>DataServerConditionMapping</code> object This
     * constructor should only be used when a default mapping is needed. This
     * mapping will not 'do any harm'. The main use for such a mapping is if the
     * default mapping happens to be missing from the statemap properties
     * defaults.
     * 
     * @param queryStatus
     */
    public DataServerConditionMapping(ExecuteQueryStatus queryStatus)
    {
        this.queryStatus = queryStatus;
        this.state = ResourceState.UNKNOWN;
        this.action = MappedAction.NONE;
        this.threshold = -1;

    }

    public DataServerConditionMapping(ExecuteQueryStatus queryStatus,
            TungstenProperties conditionProps)
    {
        this.queryStatus = queryStatus;
        this.state = ResourceState.valueOf(conditionProps.getString(STATE_KEY)
                .toUpperCase());
        this.action = MappedAction.valueOf(conditionProps.getString(ACTION_KEY)
                .toUpperCase());
        this.threshold = conditionProps.getInt(THRESHOLD_KEY);

    }

    public ExecuteQueryStatus getQueryStatus()
    {
        return queryStatus;
    }

    public void setQueryStatus(ExecuteQueryStatus queryStatus)
    {
        this.queryStatus = queryStatus;
    }

    public ResourceState getState()
    {
        return state;
    }

    public void setState(ResourceState state)
    {
        this.state = state;
    }

    public MappedAction getAction()
    {
        return action;
    }

    public void setAction(MappedAction action)
    {
        this.action = action;
    }

    public int getThreshold()
    {
        return threshold;
    }

    public void setThreshold(int threshold)
    {
        this.threshold = threshold;
    }

    public String toString()
    {
        return String.format("%s: state=%s, threshold=%d, action=%s",
                queryStatus, state, threshold, action);

    }

}
