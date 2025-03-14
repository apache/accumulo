/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.manager.tableOps.merge;

import java.io.Serializable;

import org.apache.accumulo.core.clientImpl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.clientImpl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;

public class MergeInfo implements Serializable {
  private static final long serialVersionUID = 1L;

  public enum Operation {
    MERGE, DELETE,
  }

  final TableId tableId;
  final NamespaceId namespaceId;
  private final byte[] startRow;
  private final byte[] endRow;
  private final boolean mergeRangeSet;
  private final byte[] mergeStartRow;
  private final byte[] mergeEndRow;
  final Operation op;

  public MergeInfo(TableId tableId, NamespaceId namespaceId, byte[] startRow, byte[] endRow,
      Operation op) {
    this(tableId, namespaceId, startRow, endRow, null, op);
  }

  private MergeInfo(TableId tableId, NamespaceId namespaceId, byte[] startRow, byte[] endRow,
      KeyExtent mergeRange, Operation op) {
    this.tableId = tableId;
    this.namespaceId = namespaceId;
    this.startRow = startRow;
    this.endRow = endRow;
    this.op = op;
    this.mergeRangeSet = mergeRange != null;
    if (mergeRange != null) {
      mergeStartRow =
          mergeRange.prevEndRow() == null ? null : TextUtil.getBytes(mergeRange.prevEndRow());
      mergeEndRow = mergeRange.endRow() == null ? null : TextUtil.getBytes(mergeRange.endRow());
    } else {
      mergeStartRow = null;
      mergeEndRow = null;
    }
  }

  public void validate() throws AcceptableThriftTableOperationException {
    if (startRow != null && endRow != null) {
      Text start = new Text(startRow);
      Text end = new Text(endRow);

      if (start.compareTo(end) >= 0) {
        throw new AcceptableThriftTableOperationException(tableId.canonical(), null,
            TableOperation.MERGE, TableOperationExceptionType.BAD_RANGE,
            "start row must be less than end row");
      }
    }
  }

  public KeyExtent getOriginalExtent() {
    return new KeyExtent(tableId, endRow == null ? null : new Text(endRow),
        startRow == null ? null : new Text(startRow));
  }

  public KeyExtent getMergeExtent() {
    if (mergeRangeSet) {
      return new KeyExtent(tableId, mergeEndRow == null ? null : new Text(mergeEndRow),
          mergeStartRow == null ? null : new Text(mergeStartRow));
    } else {
      return getOriginalExtent();
    }
  }

  public KeyExtent getReserveExtent() {
    switch (op) {
      case MERGE:
        return getOriginalExtent();
      case DELETE: {
        if (endRow == null) {
          return getOriginalExtent();
        } else {
          // Extend the reserve range a bit because if the last tablet is completely contained in
          // the delete range then it will be merged away. Merging the last tablet away will result
          // in modifying the next tablet, so need to reserve it.
          return new KeyExtent(tableId, new Key(endRow).followingKey(PartialKey.ROW).getRow(),
              startRow == null ? null : new Text(startRow));
        }
      }
      default:
        throw new IllegalArgumentException("unknown op " + op);
    }
  }

  public MergeInfo useMergeRange(KeyExtent mergeRange) {
    Preconditions.checkArgument(op == Operation.DELETE);
    Preconditions.checkArgument(getReserveExtent().contains(mergeRange));
    return new MergeInfo(tableId, namespaceId, startRow, endRow, mergeRange, op);
  }
}
