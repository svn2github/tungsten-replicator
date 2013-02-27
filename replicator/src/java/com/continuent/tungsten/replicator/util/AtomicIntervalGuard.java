/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2011-2013 Continuent Inc.
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

package com.continuent.tungsten.replicator.util;

import java.util.HashMap;
import java.util.Map;

import com.continuent.tungsten.replicator.ReplicatorException;

/**
 * Tracks the sequence number and time interval between a group of tasks
 * processing transactions to ensure that the first and last tasks do not get
 * too far apart in the log. Class methods are fully synchronized, which results
 * in a large number of lock requests. Changes to these classes should be
 * carefully checked for performance via unit tests.
 * <p>
 * Since the initial implementation this class has been extended to add a datum
 * that may optionally be stored with each thread. This allows clients to track
 * additional hi/low properties for themselves.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class AtomicIntervalGuard<D>
{
    // Simple class to hold task information. The id value is the key.
    // The before and after fields are used to implement a linked list of
    // threads to show ordering by sequence number.
    private class ThreadPosition
    {
        int            id;
        long           seqno;
        long           time;
        D              datum;
        long           reportTime;
        ThreadPosition before;
        ThreadPosition after;

        public String toString()
        {
            return this.getClass().getSimpleName() + " id=" + id + " seqno="
                    + seqno + " time=" + time;
        }
    }

    // Map to hold information on each thread.
    private Map<Integer, ThreadPosition> array = new HashMap<Integer, ThreadPosition>();
    private ThreadPosition               head;
    private ThreadPosition               tail;
    private int                          size;

    /**
     * Allocates a thread interval array.
     * 
     * @param size Expected number of threads for correct operation
     */
    public AtomicIntervalGuard(int size)
    {
        this.size = size;
    }

    /**
     * Report position for an individual task without added datum.
     */
    public synchronized void report(int id, long seqno, long time)
    {
        report(id, seqno, time, null);
    }

    /**
     * Report position for an individual task. This call makes an important
     * assumption that sequence numbers never move backward, which simplifies
     * maintenance of the array.
     * 
     * @param id Thread ID
     * @param seqno Sequence number reached by thread
     * @param time Original timestamp of transaction
     * @param datum An optional datum associated with the transaction
     * @throws ReplicatorException Thrown if there is an illegal update.
     */
    public synchronized void report(int id, long seqno, long time, D datum)
    {
        processReport(id, seqno, time, datum);

        // Notify anyone who is waiting.
        notifyAll();
    }

    /**
     * Report position for an individual task. This call makes an important
     * assumption that sequence numbers never move backward, which simplifies
     * maintenance of the array.
     * 
     * @param id Thread ID
     * @param seqno Sequence number reached by thread
     * @param time Original timestamp of transaction
     * @param reportTime Original time + latency
     * @param datum An optional datum associated with the transaction
     * @throws ReplicatorException Thrown if there is an illegal update.
     */
    public synchronized void report(int id, long seqno, long time,
            long reportTime, D datum)
    {
        processReport(id, seqno, time, datum);

        ThreadPosition tp = array.get(id);
        tp.reportTime = reportTime;

        // Notify anyone who is waiting.
        notifyAll();

    }

    private void processReport(int id, long seqno, long time, D datum)
    {
        ThreadPosition tp = array.get(id);

        // See if this thread is already known.
        if (tp == null)
        {
            // It is not. Allocate and add to the hash map.
            tp = new ThreadPosition();
            tp.id = id;
            tp.seqno = seqno;
            tp.time = time;
            tp.reportTime = System.currentTimeMillis();
            tp.datum = datum;
            array.put(id, tp);

            // Order within the linked list.
            if (head == null)
            {
                // We are starting a new list. This instance is now head and
                // tail.
                head = tp;
                tail = tp;
            }
            else
            {
                // We are inserting in an existing list.
                ThreadPosition nextTp = head;
                while (nextTp != null)
                {
                    // If the next item in the list has a higher sequence
                    // number, we insert before it.
                    if (nextTp.seqno > tp.seqno)
                    {
                        if (nextTp.before != null)
                            nextTp.before.after = tp;
                        tp.before = nextTp.before;
                        tp.after = nextTp;
                        nextTp.before = tp;
                        break;
                    }
                    nextTp = nextTp.after;
                }
                // If we did not find anything, we are at the tail.
                if (nextTp == null)
                {
                    tail.after = tp;
                    tp.before = tail;
                    tail = tp;
                }

                // If we do not have anything before update our position to be
                // head of the list.
                if (tp.before == null)
                    head = tp;
            }
        }
        else
        {
            // The thread is already in the map. Ensure thread seqno does not
            // move backwards and update its information.
            if (tp.seqno > seqno)
                bug("Thread reporting position moved backwards: task=" + id
                        + " previous seqno=" + tp.seqno + " new seqno=" + seqno);
            tp.seqno = seqno;
            tp.time = time;
            tp.reportTime = System.currentTimeMillis();
            tp.datum = datum;

            // Since seqno values only increase, we may need to move back in the
            // list. See if we need to move back now.
            ThreadPosition nextTp = tp.after;
            while (nextTp != null && tp.seqno > tp.after.seqno)
            {
                // First fix up nodes before and after this pair so they
                // point to each other.
                if (tp.before != null)
                    tp.before.after = nextTp;
                if (nextTp.after != null)
                    nextTp.after.before = tp;

                // Now switch the pointers on the nodes themselves.
                nextTp.before = tp.before;
                tp.after = nextTp.after;
                nextTp.after = tp;
                tp.before = nextTp;

                // See if we were at the head. If so, move the switched
                // item to the head.
                if (head == tp)
                    head = nextTp;

                // Move to the next item in the linked list.
                nextTp = tp.after;
            }

            // See if we are now at the tail. If so, update the tail.
            if (tp.after == null)
                tail = tp;
        }
    }

    /**
     * Get lowest seqno in the array.
     */
    public synchronized long getLowSeqno()
    {
        if (head == null)
            return -1;
        else
            return head.seqno;
    }

    /** Return the lowest time in the array. */
    public synchronized long getLowTime()
    {
        if (head == null)
            return -1;
        else
            return head.time;
    }

    /**
     * Return lowest latency in milliseconds between the commit time and the
     * automatically generated commit time in the array.
     */
    public synchronized long getLowLatency()
    {
        if (head == null)
            return 0;
        else
        {
            long latency = head.reportTime - head.time;
            if (latency >= 0)
                return latency;
            else
                return 0;
        }
    }

    /** Return the datum of the lowest entry in the array. */
    public synchronized D getLowDatum()
    {
        if (head == null)
            return null;
        else
            return head.datum;
    }

    /**
     * Get highest seqno in the array.
     */
    public synchronized long getHiSeqno()
    {
        if (tail == null)
            return -1;
        else
            return tail.seqno;
    }

    /** Return the highest time in the array. */
    public synchronized long getHiTime()
    {
        if (tail == null)
            return -1;
        else
            return tail.time;
    }

    /**
     * Return the latency in seconds between the commit time and the
     * automatically generated commit time.
     */
    public synchronized long getHiLatency()
    {
        if (tail == null)
            return 0;
        else
        {
            long latency = head.reportTime - head.time;
            if (latency >= 0)
                return latency;
            else
                return 0;
        }
    }

    /** Return the datum of the highest entry in the array. */
    public synchronized D getHiDatum()
    {
        if (tail == null)
            return null;
        else
            return tail.datum;
    }

    /** Return the interval between highest and lowest values. */
    public synchronized long getInterval()
    {
        return getHiTime() - getLowTime();
    }

    /**
     * Wait until the minimum time in array is greater than or equal to the
     * request time.
     * 
     * @param time Return if this time is less than or equal to the trailing
     *            commit timestamp
     * @param seqno Return if this seqno is less than or equal to the trailing
     *            seqno
     */
    public synchronized long waitMinTime(long time, long seqno)
            throws InterruptedException
    {
        assertArrayReady();
        while (time > head.time && seqno > head.seqno)
        {
            wait(1000);
        }
        return head.time;
    }

    // Assert that array is full.
    private void assertArrayReady()
    {
        if (size != array.size())
            throw new RuntimeException("Invalid access to array: size=" + size
                    + " actual=" + array.size());
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#toString()
     */
    public synchronized String toString()
    {
        StringBuffer sb = new StringBuffer();
        sb.append(this.getClass().getSimpleName());
        sb.append(" size=").append(size);
        if (array.size() > 0)
        {
            sb.append(" low_seqno=").append(head.seqno);
            sb.append(" hi_seqno=").append(tail.seqno);
            sb.append(" time_interval=").append(tail.time - head.time);
        }
        return sb.toString();
    }

    /**
     * Ensures that the array is consistent by checking various safety
     * conditions. (Where's Eiffel when you need it?)
     */
    public synchronized void validate() throws RuntimeException
    {
        if (head == null)
        {
            if (tail != null)
                bug("Head is null but tail is set");
            else if (array.size() != 0)
                bug("Array is size > 0 when head and tail are empty");
        }
        else
        {
            if (tail == null)
                bug("Head is set but not tail");
            else if (head.before != null)
                bug("Head position points to previous position: " + head.after);
            else if (tail.after != null)
                bug("Tail position points to following position: " + tail.after);
            else
            {
                ThreadPosition tp = head;
                int linkedSize;
                for (linkedSize = 1; linkedSize < array.size(); linkedSize++)
                {
                    if (tp.after == null)
                        break;
                    else
                        tp = tp.after;
                }

                if (linkedSize != array.size())
                    bug("Linked size is different from array size: linked="
                            + linkedSize + " array=" + array.size());

                if (tp != tail)
                    bug("Last item in list is not the tail: last=[" + tp
                            + "] tail=[" + tail + "]");
            }
        }
    }

    // Throw an exception with a bug message.
    private void bug(String message)
    {
        throw new RuntimeException("BUG: " + message);
    }
}