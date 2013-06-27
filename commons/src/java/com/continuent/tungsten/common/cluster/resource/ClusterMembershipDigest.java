
package com.continuent.tungsten.common.cluster.resource;

import java.io.Serializable;
import java.util.Collection;
import java.util.Vector;

import com.continuent.tungsten.common.utils.CLUtils;

public class ClusterMembershipDigest implements Serializable
{
    /**
     * 
     */
    private static final long serialVersionUID   = 1L;

    private Vector<String>    potentialMembers   = new Vector<String>();
    private Vector<String>    unvalidatedMembers = new Vector<String>();
    private Vector<String>    validMembers       = new Vector<String>();
    private Vector<String>    unreachableMembers = new Vector<String>();
    private boolean           validated          = false;

    public ClusterMembershipDigest(Collection<String> potentialMembers,
            Collection<String> unvalidatedMembers,
            Collection<String> validMembers,
            Collection<String> unreachableMembers, boolean isValidated)
    {
        if (potentialMembers != null)
        {
            this.potentialMembers.addAll(potentialMembers);
        }
        if (unvalidatedMembers != null)
        {
            this.unvalidatedMembers.addAll(unvalidatedMembers);
        }
        if (validMembers != null)
        {
            this.validMembers.addAll(validMembers);
        }
        if (unreachableMembers != null)
        {
            this.unreachableMembers.addAll(unreachableMembers);
        }

        this.setValidated(isValidated);

    }

    /**
     * Determines whether the local manager is in a primary partition, based on
     * validated membership information passed in when this class is
     * instantiated. A manager is considered to be in a primary partition if it
     * has a simple majority of a) all potential members b) at least two out of
     * three members.
     * 
     * @param verbose prints information about how the determination is being
     *            made.
     * @return true if the information indicates we are in a primary partition
     */
    public boolean isInPrimaryPartition(boolean verbose)
    {
        if (!isValidated())
        {
            CLUtils.println("WARNING: CANNOT DETERMINE PARTITION FOR UNVALIDATED DIGEST.");
            return false;
        }

        if (getPotentialMembers().size() == 0)
        {
            if (verbose)
            {
                CLUtils.println("WARNING: POTENTIAL MEMBER LIST UNAVAILABLE. RETURNING FALSE");
            }
            return false;
        }

        if (getPotentialMembers().size() < 2)
        {
            if (verbose)
            {
                CLUtils.println(String
                        .format("CANNOT ESTABLISH A MAJORITY WITH ONLY %d POTENTIAL MEMBERS",
                                getPotentialMembers().size()));
            }
            return false;
        }

        if (getUnvalidatedMembers().size() < 2)
        {
            if (verbose)
            {
                CLUtils.println(String
                        .format("CANNOT ESTABLISH A MAJORITY WITH ONLY %d UNVALIDATED MEMBERS",
                                getValidMembers().size()));
            }
            return false;

        }

        if (getValidMembers().size() < 2)
        {
            if (verbose)
            {
                CLUtils.println(String
                        .format("CANNOT ESTABLISH A MAJORITY WITH ONLY %d VALIDATED MEMBERS",
                                getValidMembers().size()));
            }
            return false;
        }

        int potentialMajorityCount = getPotentialMembers().size() / 2 + 1;

        int currentMajorityCount = getUnvalidatedMembers().size() / 2 + 1;

        int majorityCount = -1;

        int majorityBasis = -1;

        if (currentMajorityCount < potentialMajorityCount)
        {
            if (verbose)
            {
                CLUtils.println(String
                        .format("USING A MAJORITY OF THE CURRENT UNVALIDATED MEMBERSHIP: %d",
                                currentMajorityCount));
                majorityCount = currentMajorityCount;
                majorityBasis = getUnvalidatedMembers().size();
            }
        }
        else
        {
            if (verbose)
            {
                CLUtils.println(String
                        .format("USING A MAJORITY OF THE TOTAL POTENTIAL MEMBERSHIP: %d",
                                potentialMajorityCount));
                majorityCount = potentialMajorityCount;
                majorityBasis = getPotentialMembers().size();
            }
        }

        if (getValidMembers().size() >= majorityCount)
        {
            if (verbose)
            {
                CLUtils.println(String
                        .format("I AM IN A PRIMARY PARTITION OF %d MEMBERS OUT OF THE BASIS OF %d",
                                getValidMembers().size(), majorityBasis));

                CLUtils.println("VALIDATED CURRENT MEMBERS ARE:\n"
                        + CLUtils.iterableToString(getValidMembers()));
            }
            return true;
        }

        if (verbose)
        {
            CLUtils.println(String
                    .format("I AM IN A NON-PRIMARY PARTITION OF %d MEMBERS OUT OF A BASIS OF %d",
                            getValidMembers().size(), majorityBasis));
            CLUtils.println("VALIDATED CURRENT MEMBERS ARE:\n"
                    + CLUtils.iterableToString(getValidMembers()));
            CLUtils.println("UNREACHABLE MEMBERS ARE:\n"
                    + CLUtils.iterableToString(getUnreachableMembers()));

        }
        return false;

    }

    public boolean isValidMembership(boolean verbose)
    {
        if (unvalidatedMembers.size() > 0 && validMembers.size() > 0
                && unvalidatedMembers.size() == validMembers.size())
        {
            if (verbose)
            {
                CLUtils.println("MEMBERSHIP IS CONSISTENT");
                CLUtils.println("GC VIEW OF CURRENT MEMBERS IS:\n"
                        + CLUtils.iterableToString(getUnvalidatedMembers()));
                CLUtils.println("VALIDATED CURRENT MEMBERS ARE:\n"
                        + CLUtils.iterableToString(getValidMembers()));
            }
            return true;
        }

        if (verbose)
        {
            CLUtils.println("MEMBERSHIP IS NOT CONSISTENT");
            CLUtils.println("GC VIEW OF CURRENT MEMBERS IS:\n"
                    + CLUtils.iterableToString(getUnvalidatedMembers()));
            CLUtils.println("VALIDATED CURRENT MEMBERS ARE:\n"
                    + CLUtils.iterableToString(getValidMembers()));
            CLUtils.println("UNREACHABLE MEMBERS ARE:\n"
                    + CLUtils.iterableToString(getUnreachableMembers()));
        }
        return false;
    }

    /**
     * Returns the potentialMembers value.
     * 
     * @return Returns the potentialMembers.
     */
    public Vector<String> getPotentialMembers()
    {
        return potentialMembers;
    }

    /**
     * Sets the potentialMembers value.
     * 
     * @param potentialMembers The potentialMembers to set.
     */
    public void setPotentialMembers(Vector<String> potentialMembers)
    {
        this.potentialMembers = potentialMembers;
    }

    /**
     * Returns the validMembers value.
     * 
     * @return Returns the validMembers.
     */
    public Vector<String> getValidMembers()
    {
        return validMembers;
    }

    /**
     * Sets the validMembers value.
     * 
     * @param validMembers The validMembers to set.
     */
    public void setValidMembers(Vector<String> validMembers)
    {
        this.validMembers = validMembers;
    }

    /**
     * Returns the unreachableMembers value.
     * 
     * @return Returns the unreachableMembers.
     */
    public Vector<String> getUnreachableMembers()
    {
        return unreachableMembers;
    }

    /**
     * Sets the unreachableMembers value.
     * 
     * @param unreachableMembers The unreachableMembers to set.
     */
    public void setUnreachableMembers(Vector<String> unreachableMembers)
    {
        this.unreachableMembers = unreachableMembers;
    }

    /**
     * Returns the unvalidatedMembers value.
     * 
     * @return Returns the unvalidatedMembers.
     */
    public Vector<String> getUnvalidatedMembers()
    {
        return unvalidatedMembers;
    }

    /**
     * Sets the unvalidatedMembers value.
     * 
     * @param unvalidatedMembers The unvalidatedMembers to set.
     */
    public void setUnvalidatedMembers(Vector<String> unvalidatedMembers)
    {
        this.unvalidatedMembers = unvalidatedMembers;
    }

    /**
     * Returns the validated value.
     * 
     * @return Returns the validated.
     */
    public boolean isValidated()
    {
        return validated;
    }

    /**
     * Sets the validated value.
     * 
     * @param validated The validated to set.
     */
    public void setValidated(boolean validated)
    {
        this.validated = validated;
    }

}
