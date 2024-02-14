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

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.manager.tableOps.Utils;
import org.apache.accumulo.server.compaction.CompactionConfigStorage;
import org.apache.zookeeper.KeeperException;

public class PreDeleteTable extends ManagerRepo {

  public static String createDeleteMarkerPath(InstanceId instanceId, TableId tableId) {
    return Constants.ZROOT + "/" + instanceId + Constants.ZTABLES + "/" + tableId.canonical()
        + Constants.ZTABLE_DELETE_MARKER;
  }

  private static final long serialVersionUID = 1L;

  private TableId tableId;
  private NamespaceId namespaceId;

  public PreDeleteTable(NamespaceId namespaceId, TableId tableId) {
    this.tableId = tableId;
    this.namespaceId = namespaceId;
  }

  @Override
  public long isReady(FateId fateId, Manager env) throws Exception {
    return Utils.reserveNamespace(env, namespaceId, fateId, false, true, TableOperation.DELETE)
        + Utils.reserveTable(env, tableId, fateId, false, true, TableOperation.DELETE);
  }

  private void preventFutureCompactions(Manager environment)
      throws KeeperException, InterruptedException {
    // ELASTICITY_TODO investigate this. Is still needed? Is it still working as expected?
    String deleteMarkerPath = createDeleteMarkerPath(environment.getInstanceID(), tableId);
    ZooReaderWriter zoo = environment.getContext().getZooReaderWriter();
    zoo.putPersistentData(deleteMarkerPath, new byte[] {}, NodeExistsPolicy.SKIP);
  }

  @Override
  public Repo<Manager> call(FateId fateId, Manager environment) throws Exception {
    try {
      preventFutureCompactions(environment);

      var idsToCancel =
          CompactionConfigStorage.getAllConfig(environment.getContext(), tableId::equals).keySet();

      for (var idToCancel : idsToCancel) {
        CompactionConfigStorage.deleteConfig(environment.getContext(), idToCancel);
      }
      return new DeleteTable(namespaceId, tableId);
    } finally {
      Utils.unreserveTable(environment, tableId, fateId, false);
      Utils.unreserveNamespace(environment, namespaceId, fateId, false);
    }
  }

  @Override
  public void undo(FateId fateId, Manager env) {
    Utils.unreserveTable(env, tableId, fateId, false);
    Utils.unreserveNamespace(env, namespaceId, fateId, false);
  }

}
