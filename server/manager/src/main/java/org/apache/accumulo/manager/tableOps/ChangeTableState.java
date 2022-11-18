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
package org.apache.accumulo.manager.tableOps;

import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.manager.Manager;
import org.slf4j.LoggerFactory;

public class ChangeTableState extends ManagerRepo {

  private static final long serialVersionUID = 1L;
  private TableId tableId;
  private NamespaceId namespaceId;
  private TableOperation top;

  public ChangeTableState(NamespaceId namespaceId, TableId tableId, TableOperation top) {
    this.tableId = tableId;
    this.namespaceId = namespaceId;
    this.top = top;

    if (top != TableOperation.ONLINE && top != TableOperation.OFFLINE) {
      throw new IllegalArgumentException(top.toString());
    }
  }

  @Override
  public long isReady(long tid, Manager env) throws Exception {
    // reserve the table so that this op does not run concurrently with create, clone, or delete
    // table
    return Utils.reserveNamespace(env, namespaceId, tid, false, true, top)
        + Utils.reserveTable(env, tableId, tid, true, true, top);
  }

  @Override
  public Repo<Manager> call(long tid, Manager env) {
    TableState ts = TableState.ONLINE;
    if (top == TableOperation.OFFLINE) {
      ts = TableState.OFFLINE;
    }

    env.getTableManager().transitionTableState(tableId, ts);
    Utils.unreserveNamespace(env, namespaceId, tid, false);
    Utils.unreserveTable(env, tableId, tid, true);
    LoggerFactory.getLogger(ChangeTableState.class).debug("Changed table state {} {}", tableId, ts);
    env.getEventCoordinator().event("Set table state of %s to %s", tableId, ts);
    return null;
  }

  @Override
  public void undo(long tid, Manager env) {
    Utils.unreserveNamespace(env, namespaceId, tid, false);
    Utils.unreserveTable(env, tableId, tid, true);
  }
}
