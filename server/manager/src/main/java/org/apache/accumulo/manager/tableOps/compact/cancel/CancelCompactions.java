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
package org.apache.accumulo.manager.tableOps.compact.cancel;

import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.FateTxId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.manager.tableOps.Utils;
import org.apache.accumulo.server.compaction.CompactionConfigStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CancelCompactions extends ManagerRepo {

  private static final long serialVersionUID = 1L;
  private TableId tableId;
  private NamespaceId namespaceId;

  private static final Logger log = LoggerFactory.getLogger(CancelCompactions.class);

  public CancelCompactions(NamespaceId namespaceId, TableId tableId) {
    this.tableId = tableId;
    this.namespaceId = namespaceId;
  }

  @Override
  public long isReady(FateId fateId, Manager env) throws Exception {
    return Utils.reserveNamespace(env, namespaceId, fateId, false, true,
        TableOperation.COMPACT_CANCEL)
        + Utils.reserveTable(env, tableId, fateId, false, true, TableOperation.COMPACT_CANCEL);
  }

  @Override
  public Repo<Manager> call(FateId fateId, Manager environment) throws Exception {
    // ELASTICITY_TODO DEFERRED - ISSUE 4044
    var idsToCancel =
        CompactionConfigStorage.getAllConfig(environment.getContext(), tableId::equals).keySet();

    for (var idToCancel : idsToCancel) {
      log.debug("{} deleting compaction config {}", fateId, FateTxId.formatTid(idToCancel));
      CompactionConfigStorage.deleteConfig(environment.getContext(), idToCancel);
    }
    return new FinishCancelCompaction(namespaceId, tableId);
  }

  @Override
  public void undo(FateId fateId, Manager env) {
    Utils.unreserveTable(env, tableId, fateId, false);
    Utils.unreserveNamespace(env, namespaceId, fateId, false);
  }

}
