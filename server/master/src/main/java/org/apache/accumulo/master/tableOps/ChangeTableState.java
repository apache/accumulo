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

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.thrift.TableOperation;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.tables.TableManager;
import org.apache.log4j.Logger;

public class ChangeTableState extends MasterRepo {

  private static final long serialVersionUID = 1L;
  private String tableId;
  private TableOperation top;
  private String namespaceId;

  public ChangeTableState(String tableId, TableOperation top) {
    this.tableId = tableId;
    this.top = top;
    Instance inst = HdfsZooInstance.getInstance();
    this.namespaceId = Tables.getNamespaceId(inst, tableId);

    if (top != TableOperation.ONLINE && top != TableOperation.OFFLINE)
      throw new IllegalArgumentException(top.toString());
  }

  @Override
  public long isReady(long tid, Master environment) throws Exception {
    // reserve the table so that this op does not run concurrently with create, clone, or delete table
    return Utils.reserveNamespace(namespaceId, tid, false, true, top) + Utils.reserveTable(tableId, tid, true, true, top);
  }

  @Override
  public Repo<Master> call(long tid, Master env) throws Exception {

    TableState ts = TableState.ONLINE;
    if (top == TableOperation.OFFLINE)
      ts = TableState.OFFLINE;

    TableManager.getInstance().transitionTableState(tableId, ts);
    Utils.unreserveNamespace(namespaceId, tid, false);
    Utils.unreserveTable(tableId, tid, true);
    Logger.getLogger(ChangeTableState.class).debug("Changed table state " + tableId + " " + ts);
    env.getEventCoordinator().event("Set table state of %s to %s", tableId, ts);
    return null;
  }

  @Override
  public void undo(long tid, Master env) throws Exception {
    Utils.unreserveNamespace(namespaceId, tid, false);
    Utils.unreserveTable(tableId, tid, true);
  }
}
