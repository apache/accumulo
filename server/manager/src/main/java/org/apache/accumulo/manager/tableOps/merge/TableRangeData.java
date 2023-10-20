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
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.hadoop.io.Text;

public class TableRangeData implements Serializable {
  private static final long serialVersionUID = 1L;

  final TableId tableId;
  final NamespaceId namespaceId;
  private final byte[] startRow;
  private final byte[] endRow;
  final MergeInfo.Operation op;

  public TableRangeData(TableId tableId, NamespaceId namespaceId, byte[] startRow, byte[] endRow,
      MergeInfo.Operation op) {
    this.tableId = tableId;
    this.namespaceId = namespaceId;
    this.startRow = startRow;
    this.endRow = endRow;
    this.op = op;

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

  public MergeInfo getMergeInfo() {
    return new MergeInfo(new KeyExtent(tableId, endRow == null ? null : new Text(endRow),
        startRow == null ? null : new Text(startRow)), op);
  }
}
