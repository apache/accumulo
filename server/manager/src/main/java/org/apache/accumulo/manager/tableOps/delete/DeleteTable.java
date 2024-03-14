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

import java.util.EnumSet;

import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.fate.zookeeper.DistributedReadWriteLock.LockType;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.manager.tableOps.Utils;

public class DeleteTable extends ManagerRepo {

  private static final long serialVersionUID = 1L;

  private TableId tableId;
  private NamespaceId namespaceId;

  public DeleteTable(NamespaceId namespaceId, TableId tableId) {
    this.namespaceId = namespaceId;
    this.tableId = tableId;
  }

  @Override
  public long isReady(FateId fateId, Manager env) throws Exception {
    return Utils.reserveNamespace(env, namespaceId, fateId, LockType.READ, false,
        TableOperation.DELETE)
        + Utils.reserveTable(env, tableId, fateId, LockType.WRITE, true, TableOperation.DELETE);
  }

  @Override
  public Repo<Manager> call(FateId fateId, Manager env) {
    final EnumSet<TableState> expectedCurrStates =
        EnumSet.of(TableState.ONLINE, TableState.OFFLINE);
    env.getTableManager().transitionTableState(tableId, TableState.DELETING, expectedCurrStates);
    env.getEventCoordinator().event(tableId, "deleting table %s %s", tableId, fateId);
    return new ReserveTablets(tableId, namespaceId);
  }

  @Override
  public void undo(FateId fateId, Manager env) {
    Utils.unreserveTable(env, tableId, fateId, LockType.WRITE);
    Utils.unreserveNamespace(env, namespaceId, fateId, LockType.READ);
  }
}
