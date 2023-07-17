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

import java.util.Optional;

import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.clientImpl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.clientImpl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.manager.tableOps.Utils;
import org.apache.accumulo.server.compaction.CompactionConfigStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompactRange extends ManagerRepo {
  private static final Logger log = LoggerFactory.getLogger(CompactRange.class);

  private static final long serialVersionUID = 1L;
  private final TableId tableId;
  private final NamespaceId namespaceId;
  private byte[] startRow;
  private byte[] endRow;
  private byte[] config;

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
  public long isReady(long tid, Manager env) throws Exception {
    return Utils.reserveNamespace(env, namespaceId, tid, false, true, TableOperation.COMPACT)
        + Utils.reserveTable(env, tableId, tid, false, true, TableOperation.COMPACT);
  }

  @Override
  public Repo<Manager> call(final long tid, Manager env) throws Exception {
    CompactionConfigStorage.setConfig(env.getContext(), tid, config);
    return new CompactionDriver(namespaceId, tableId, startRow, endRow);
  }

  @Override
  public void undo(long tid, Manager env) throws Exception {
    try {
      CompactionConfigStorage.deleteConfig(env.getContext(), tid);
    } finally {
      Utils.unreserveNamespace(env, namespaceId, tid, false);
      Utils.unreserveTable(env, tableId, tid, false);
    }
  }

}
