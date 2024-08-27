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
package org.apache.accumulo.manager.tableOps.compact;

import static java.util.Objects.requireNonNull;

import java.util.Objects;
import java.util.Optional;

import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.clientImpl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.clientImpl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.fate.zookeeper.DistributedReadWriteLock.LockType;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.manager.tableOps.Utils;
import org.apache.accumulo.server.compaction.CompactionConfigStorage;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.MoreCollectors;

public class CompactRange extends ManagerRepo {
  private static final Logger log = LoggerFactory.getLogger(CompactRange.class);

  private static final long serialVersionUID = 1L;
  private final TableId tableId;
  private final NamespaceId namespaceId;
  private byte[] startRow;
  private byte[] endRow;
  private final byte[] config;

  public CompactRange(NamespaceId namespaceId, TableId tableId, CompactionConfig compactionConfig)
      throws AcceptableThriftTableOperationException {

    requireNonNull(namespaceId, "Invalid argument: null namespaceId");
    requireNonNull(tableId, "Invalid argument: null tableId");
    requireNonNull(compactionConfig, "Invalid argument: null compaction config");

    this.tableId = tableId;
    this.namespaceId = namespaceId;
    this.config = CompactionConfigStorage.encodeConfig(compactionConfig, tableId);

    if (compactionConfig.getStartRow() != null && compactionConfig.getEndRow() != null
        && compactionConfig.getStartRow().compareTo(compactionConfig.getEndRow()) >= 0) {
      throw new AcceptableThriftTableOperationException(tableId.canonical(), null,
          TableOperation.COMPACT, TableOperationExceptionType.BAD_RANGE,
          "start row must be less than end row");
    }

    this.startRow =
        Optional.ofNullable(compactionConfig.getStartRow()).map(TextUtil::getBytes).orElse(null);
    this.endRow =
        Optional.ofNullable(compactionConfig.getEndRow()).map(TextUtil::getBytes).orElse(null);
  }

  @Override
  public long isReady(FateId fateId, Manager env) throws Exception {
    return Utils.reserveNamespace(env, namespaceId, fateId, LockType.READ, true,
        TableOperation.COMPACT)
        + Utils.reserveTable(env, tableId, fateId, LockType.READ, true, TableOperation.COMPACT);
  }

  @Override
  public Repo<Manager> call(final FateId fateId, Manager env) throws Exception {
    CompactionConfigStorage.setConfig(env.getContext(), fateId, config);
    KeyExtent keyExtent;
    byte[] prevRowOfStartRowTablet = startRow;
    byte[] endRowOfEndRowTablet = endRow;

    if (startRow != null) {
      // The startRow in a compaction range is not inclusive, so do not want to find the tablet
      // containing startRow but instead find the tablet that contains the next possible row after
      // startRow
      Text nextPossibleRow = new Key(startRow).followingKey(PartialKey.ROW).getRow();
      keyExtent = findContaining(env.getContext().getAmple(), tableId, nextPossibleRow);
      prevRowOfStartRowTablet =
          keyExtent.prevEndRow() == null ? null : TextUtil.getBytes(keyExtent.prevEndRow());
    }

    if (endRow != null) {
      // find the tablet containing endRow and pass its end row to the CompactionDriver constructor.
      keyExtent = findContaining(env.getContext().getAmple(), tableId, new Text(endRow));
      endRowOfEndRowTablet =
          keyExtent.endRow() == null ? null : TextUtil.getBytes(keyExtent.endRow());
    }
    return new CompactionDriver(namespaceId, tableId, prevRowOfStartRowTablet,
        endRowOfEndRowTablet);
  }

  private static KeyExtent findContaining(Ample ample, TableId tableId, Text row) {
    Objects.requireNonNull(row);
    try (var tablets = ample.readTablets().forTable(tableId).overlapping(row, true, row)
        .fetch(TabletMetadata.ColumnType.PREV_ROW).build()) {
      return tablets.stream().collect(MoreCollectors.onlyElement()).getExtent();

    }

  }

  @Override
  public void undo(FateId fateId, Manager env) throws Exception {
    try {
      CompactionConfigStorage.deleteConfig(env.getContext(), fateId);
    } finally {
      Utils.unreserveNamespace(env, namespaceId, fateId, LockType.READ);
      Utils.unreserveTable(env, tableId, fateId, LockType.READ);
    }
  }

}
