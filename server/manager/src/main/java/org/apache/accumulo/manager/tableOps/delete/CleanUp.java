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
package org.apache.accumulo.manager.tableOps.delete;

import static org.apache.accumulo.core.util.LazySingletons.GSON;

import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.fate.zookeeper.DistributedReadWriteLock.LockType;
import org.apache.accumulo.manager.tableOps.AbstractFateOperation;
import org.apache.accumulo.manager.tableOps.FateEnv;
import org.apache.accumulo.manager.tableOps.Utils;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonObject;

class CleanUp extends AbstractFateOperation {

  private static final Logger log = LoggerFactory.getLogger(CleanUp.class);

  private static final long serialVersionUID = 1L;

  private final TableId tableId;
  private final NamespaceId namespaceId;

  public CleanUp(TableId tableId, NamespaceId namespaceId) {
    this.tableId = tableId;
    this.namespaceId = namespaceId;
  }

  @Override
  public Repo<FateEnv> call(FateId fateId, FateEnv env) {

    // remove metadata table entries
    try {
      // Intentionally do not pass manager lock. If manager loses lock, this operation may complete
      // before manager can kill itself.
      // If the manager lock passed to deleteTable, it is possible that the delete mutations will be
      // dropped. If the delete operations
      // are dropped and the operation completes, then the deletes will not be repeated.
      MetadataTableUtil.deleteTable(tableId, true, env.getContext(), null);
    } catch (Exception e) {
      log.error("error deleting {} from metadata table", tableId, e);
    }

    // remove table from zookeeper
    try {
      env.getTableManager().removeTable(tableId, namespaceId);
      env.getContext().clearTableListCache();
    } catch (Exception e) {
      log.error("Failed to find table id {} in zookeeper", tableId, e);
    }

    // remove any permissions associated with this table
    try {
      env.getContext().getSecurityOperation().deleteTable(env.getContext().rpcCreds(), tableId,
          namespaceId);
    } catch (ThriftSecurityException e) {
      log.error("{}", e.getMessage(), e);
    }

    Utils.unreserveTable(env.getContext(), tableId, fateId, LockType.WRITE);
    Utils.unreserveNamespace(env.getContext(), namespaceId, fateId, LockType.READ);

    log.debug("Deleted table {}", tableId);

    return null;
  }

  @Override
  public void undo(FateId fateId, FateEnv environment) {
    // nothing to do
  }

  @Override
  public String getDetails() {
    JsonObject details = new JsonObject();
    details.addProperty("namespaceId", namespaceId.canonical());
    details.addProperty("tableId", tableId.canonical());
    return GSON.get().toJson(details);
  }
}
