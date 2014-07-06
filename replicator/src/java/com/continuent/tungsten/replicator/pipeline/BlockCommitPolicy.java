/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2014 Continuent Inc.
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

package com.continuent.tungsten.replicator.pipeline;

/**
 * Defines the special policies for block commit. Blocks generally commit when
 * they exceed the blockCommitRowCount or blockCommitInterval. These settings
 * specify additional policies for committing before the end of a block.
 */
public enum BlockCommitPolicy
{
    /**
     * Commit block only on end of block *or* commit_immediate tag.
     */
    lax,
    /**
     * Commit block immediately on fragmented transactions, service changes and
     * transactions tagged with unsafe_for_block_commit.
     */
    strict
}