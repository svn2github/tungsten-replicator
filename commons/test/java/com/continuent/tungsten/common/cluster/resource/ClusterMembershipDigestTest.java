/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2011 Continuent Inc.
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

package com.continuent.tungsten.common.cluster.resource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import junit.framework.Assert;

import org.junit.Test;

import com.continuent.tungsten.common.utils.CLUtils;

/**
 * Implements a unit test of IndexedLRUCache features.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class ClusterMembershipDigestTest
{
    /**
     * Verify that we can create a membership digest instance that returns a
     * correctly computed quorum set and witnesses.
     */
    @Test
    public void testInstantiation() throws Exception
    {
        List<String> configured = Arrays.asList("a", "b");
        List<String> view = Arrays.asList("b", "c", "d");
        List<String> witnesses = Arrays.asList("e");
        ClusterMembershipDigest digest = new ClusterMembershipDigest("myname",
                configured, view, witnesses);

        // Test that values are correctly returned.
        List<String> quorumSet = Arrays.asList("a", "b", "c", "d");
        Assert.assertEquals("myname", digest.getName());
        Assert.assertEquals("Configured member size", 2, digest
                .getConfiguredSetMembers().size());
        Assert.assertEquals("Configured view size", 3, digest
                .getViewSetMembers().size());
        Assert.assertEquals("Witness set size", 1, digest
                .getWitnessSetMembers().size());
        assertEqualSet("Quorum set", quorumSet, digest.getPotentialQuorumMembersSetNames());
    }

    /**
     * Verify that the digest correctly indicates membership is valid if all
     * members are validated and otherwise returns false.
     */
    @Test
    public void testMembershipValidity() throws Exception
    {
        List<String> configured = Arrays.asList("a", "b");
        List<String> view = Arrays.asList("b", "c", "d");
        ClusterMembershipDigest digest = new ClusterMembershipDigest("myname",
                configured, view, null);

        // Assert that the membership is invalid as long as not all members are
        // validated.
        for (String member : view)
        {
            Assert.assertFalse("Before member validated: " + member,
                    digest.isValidMembership(false));
            digest.setValidated(member, true);
        }

        // Now it should be valid.
        Assert.assertTrue("All members are valid",
                digest.isValidMembership(false));
    }

    /**
     * Verify that a quorum set of one validated node is a primary partition.
     */
    @Test
    public void testMajorityOfOne() throws Exception
    {
        List<String> configured = Arrays.asList("a");
        List<String> view = Arrays.asList("a");
        ClusterMembershipDigest digest = new ClusterMembershipDigest("a",
                configured, view, null);
        // If we have not validated the node, we don't have a majority.
        Assert.assertFalse("Unvalidated member cannot create majority",
                digest.isInPrimaryPartition(true));

        // Once the node is validated, we have a majority.
        digest.setValidated("a", true);
        Assert.assertTrue("Single validated member constitutes a majority",
                digest.isInPrimaryPartition(true));
    }

    /**
     * Verify that a quorum set is a primary partition if there is a simple
     * majority of nodes where all nodes in the view are validated.
     */
    @Test
    public void testSimpleMajority() throws Exception
    {
        List<String> configured = Arrays.asList("a", "b", "c");
        List<String> view = Arrays.asList("a", "b");
        ClusterMembershipDigest digest = new ClusterMembershipDigest("a",
                configured, view, null);

        // If we have not validated a majority, we are not in a primary
        // partition.
        Assert.assertFalse("0 of 3 validated is not majority",
                digest.isInPrimaryPartition(true));
        
        // I need to be sure that 'myself' is validated. But that's not enough
        // for a majority.
        CLUtils.println("About to set validated for a");
        digest.setValidated("a", true);
        Assert.assertFalse("1 of 3 validated is not majority",
                digest.isInPrimaryPartition(true));

        // 2 of 3 is a majority.
        digest.setValidated("b", true);
        Assert.assertTrue("2 of 3 validated is a majority",
                digest.isInPrimaryPartition(true));
       
        // Validating a member that does not appear in the view
        // should cause an error
        digest.setValidated("c", true);
        Assert.assertFalse("3 validated out of a view size of 2 is invalid",
                digest.isInPrimaryPartition(true));
        
        
        /*
         * Test for a majority of four configured members.
         */
        configured = Arrays.asList("a", "b", "c", "d");
        view = Arrays.asList("a", "b", "c");
        digest = new ClusterMembershipDigest("a",
                configured, view, null);
        
        // I need to be sure that 'myself' is validated. But that's not enough
        // for a majority.
        digest.setValidated("a", true);
        Assert.assertFalse("1 of 4 validated is not majority",
                digest.isInPrimaryPartition(true));

        // 2 of 4 is not a majority.
        digest.setValidated("b", true);
        Assert.assertFalse("2 of 4 validated is not a majority",
                digest.isInPrimaryPartition(true));
        
        // 3 of 4 is a majority.
        digest.setValidated("c", true);
        Assert.assertTrue("3 of 4 validated is a majority",
                digest.isInPrimaryPartition(true));

    }

    /**
     * Verify that a quorum set with an even number of validate nodes plus a
     * reachable witness is a primary partition.
     */
    @Test
    public void testWitness() throws Exception
    {
        List<String> configured = Arrays.asList("a", "b");
        List<String> view = Arrays.asList("a");
        List<String> witnesses = Arrays.asList("c", "d");
        ClusterMembershipDigest digest = new ClusterMembershipDigest("a",
                configured, view, witnesses);

        // Always validate ourself, but that is not enough for a quorum...
        digest.setValidated("a", true);
        Assert.assertFalse(
                "1 of 2 validated without witnesses is not majority",
                digest.isInPrimaryPartition(true));
        
        digest.setReachable("c", true);
        Assert.assertFalse(
                "1 of 2 validated without all witnesses reachable is not majority",
                digest.isInPrimaryPartition(true));

        // 1 of 2 with all reachable witnesses is a majority.
        digest.setReachable("d", true);
        Assert.assertTrue(
                "1 of 2 validated with all witnesses reachable is majority",
                digest.isInPrimaryPartition(true));

        // To be thorough ensure we properly fail if witnesses are null.
        ClusterMembershipDigest digest2 = new ClusterMembershipDigest("a",
                configured, view, null);
        digest2.setValidated("a", true);
        Assert.assertFalse(
                "1 of 2 validated with null witnesses is not majority",
                digest2.isInPrimaryPartition(true));
    }

    /**
     * Verify that invalid configurations are properly caught. This includes a
     * configuration where the member name is not included in the quorum set,
     * where the quorum set is empty, or where the view is empty.
     */
    @Test
    public void testInvalidConfigurations() throws Exception
    {
        List<String> configured = Arrays.asList("a", "b");
        List<String> view = Arrays.asList("a", "b");

        // Confirm that member name must be in quorum set.
        ClusterMembershipDigest badMemberName = new ClusterMembershipDigest(
                "c", configured, view, null);
        Assert.assertFalse("Member must be in quorum set",
                badMemberName.isValidPotentialQuorumMembersSet(true));

        // Confirm that quorum set must be non-null.
        ClusterMembershipDigest emptyQuorum = new ClusterMembershipDigest("a",
                new ArrayList<String>(), null, null);
        Assert.assertFalse("Quorum set must non-null",
                emptyQuorum.isValidPotentialQuorumMembersSet(true));

        // Confirm that GC view contains members.
        ClusterMembershipDigest emptyView = new ClusterMembershipDigest("a",
                configured, null, null);
        Assert.assertFalse("View must have members",
                emptyView.isValidPotentialQuorumMembersSet(true));

        // Confirm that GC view contains the member name.
        ClusterMembershipDigest memberNotInView = new ClusterMembershipDigest(
                "a", configured, Arrays.asList("b"), null);
        Assert.assertFalse("View must contain the member name",
                memberNotInView.isValidPotentialQuorumMembersSet(true));

        // Confirm that configured list has at least one member.
        ClusterMembershipDigest noConfiguredNames = new ClusterMembershipDigest(
                "a", null, Arrays.asList("a"), null);
        Assert.assertFalse("Configured names must include at least one name",
                noConfiguredNames.isValidPotentialQuorumMembersSet(true));
    }

    // Assert that two lists contain identical members.
    private void assertEqualSet(String message, List<String> first,
            List<String> second)
    {
        Set<String> a = new TreeSet<String>(first);
        Set<String> b = new TreeSet<String>(second);
        Assert.assertTrue(message, a.equals(b));
    }
}