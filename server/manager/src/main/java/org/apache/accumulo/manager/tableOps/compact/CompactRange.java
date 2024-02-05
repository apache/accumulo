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
import org.apache.accumulo.core.fate.FateId;
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
  public long isReady(FateId fateId, Manager env) throws Exception {
    return Utils.reserveNamespace(env, namespaceId, fateId, false, true, TableOperation.COMPACT)
        + Utils.reserveTable(env, tableId, fateId, false, true, TableOperation.COMPACT);
  }

  @Override
  public Repo<Manager> call(final FateId fateId, Manager env) throws Exception {
    // ELASTICITY_TODO DEFERRED - ISSUE 4044
    CompactionConfigStorage.setConfig(env.getContext(), fateId.getTid(), config);
    return new CompactionDriver(namespaceId, tableId, startRow, endRow);
  }

  @Override
  public void undo(FateId fateId, Manager env) throws Exception {
    try {
      // ELASTICITY_TODO DEFERRED - ISSUE 4044
      CompactionConfigStorage.deleteConfig(env.getContext(), fateId.getTid());
    } finally {
      Utils.unreserveNamespace(env, namespaceId, fateId, false);
      Utils.unreserveTable(env, tableId, fateId, false);
    }
  }

}
