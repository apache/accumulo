/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.master.tableOps;

import org.apache.accumulo.core.client.impl.Namespace;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.client.impl.thrift.TableOperation;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.tables.TableManager;

public class DeleteTable extends MasterRepo {

  private static final long serialVersionUID = 1L;

  private Table.ID tableId;
  private Namespace.ID namespaceId;

  public DeleteTable(Namespace.ID namespaceId, Table.ID tableId) {
    this.namespaceId = namespaceId;
    this.tableId = tableId;
  }

  @Override
  public long isReady(long tid, Master env) throws Exception {
    return Utils.reserveNamespace(namespaceId, tid, false, false, TableOperation.DELETE) + Utils.reserveTable(tableId, tid, true, true, TableOperation.DELETE);
  }

  @Override
  public Repo<Master> call(long tid, Master env) throws Exception {
    TableManager.getInstance().transitionTableState(tableId, TableState.DELETING);
    env.getEventCoordinator().event("deleting table %s ", tableId);
    return new CleanUp(tableId, namespaceId);
  }

  @Override
  public void undo(long tid, Master env) throws Exception {
    Utils.unreserveTable(tableId, tid, true);
    Utils.unreserveNamespace(namespaceId, tid, false);
  }
}
