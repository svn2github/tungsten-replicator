/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2009-2013 Continuent Inc.
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
 * Initial developer(s): Seppo Jaakola
 * Contributor(s): Stephane Giron
 */

package com.continuent.tungsten.replicator.extractor.mysql;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.extractor.ExtractorException;

/**
 * @author <a href="mailto:seppo.jaakola@continuent.com">Seppo Jaakola</a>
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class DeleteRowsLogEvent extends RowsLogEvent
{

    public DeleteRowsLogEvent(byte[] buffer, int eventLength,
            FormatDescriptionLogEvent descriptionEvent,
            boolean useBytesForString, String currentPosition)
            throws ReplicatorException
    {
        super(buffer, eventLength, descriptionEvent,
                buffer[MysqlBinlog.EVENT_TYPE_OFFSET], useBytesForString);

        this.startPosition = currentPosition;
        if (logger.isDebugEnabled())
            logger.debug("Extracting event at position  : " + startPosition
                    + " -> " + getNextEventPosition());
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.mysql.RowsLogEvent#processExtractedEvent(com.continuent.tungsten.replicator.dbms.RowChangeData,
     *      com.continuent.tungsten.replicator.extractor.mysql.TableMapLogEvent)
     */
    @Override
    public void processExtractedEvent(RowChangeData rowChanges,
            TableMapLogEvent map) throws ReplicatorException
    {
        if (map == null)
        {
            throw new MySQLExtractException(
                    "Delete row event for unknown table");
        }
        OneRowChange oneRowChange = new OneRowChange();
        oneRowChange.setSchemaName(map.getDatabaseName());
        oneRowChange.setTableName(map.getTableName());
        oneRowChange.setTableId(map.getTableId());
        oneRowChange.setAction(RowChangeData.ActionType.DELETE);

        int rowIndex = 0; /* index of the row in value arrays */

        int size = bufferSize;

        for (int i = 0; i < size;)
        {
            int length = 0;

            try
            {
                length = processExtractedEventRow(oneRowChange, rowIndex,
                        usedColumns, i, packedRowsBuffer, map, true);
            }
            catch (ExtractorException e)
            {
                throw (e);
            }
            rowIndex++;

            if (length == 0)
                break;
            i += length;
        }
        rowChanges.appendOneRowChange(oneRowChange);

        // Store options, if any
        rowChanges.addOption("foreign_key_checks", getForeignKeyChecksFlag());
        rowChanges.addOption("unique_checks", getUniqueChecksFlag());
    }
}
