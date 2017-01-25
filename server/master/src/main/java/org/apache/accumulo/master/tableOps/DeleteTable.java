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

import org.apache.accumulo.core.client.impl.thrift.TableOperation;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.tables.TableManager;

public class DeleteTable extends MasterRepo {

  private static final long serialVersionUID = 1L;

  private String tableId;
  private String namespaceId;

  private String getNamespaceId(Master environment) throws Exception {
    if (namespaceId == null) {
      // For ACCUMULO-4575 namespaceId was added in a bug fix release. Since it was added in bug fix release, we have to ensure we can properly deserialize
      // older versions. When deserializing an older version, namespaceId will be null. For this case revert to the old buggy behavior.
      return Utils.getNamespaceId(environment.getInstance(), tableId, TableOperation.DELETE);
    }

    return namespaceId;
  }

  public DeleteTable(String namespaceId, String tableId) {
    this.namespaceId = namespaceId;
    this.tableId = tableId;
  }

  @Override
  public long isReady(long tid, Master environment) throws Exception {
    String namespaceId = getNamespaceId(environment);
    return Utils.reserveNamespace(namespaceId, tid, false, false, TableOperation.DELETE) + Utils.reserveTable(tableId, tid, true, true, TableOperation.DELETE);
  }

  @Override
  public Repo<Master> call(long tid, Master environment) throws Exception {
    String namespaceId = getNamespaceId(environment);
    TableManager.getInstance().transitionTableState(tableId, TableState.DELETING);
    environment.getEventCoordinator().event("deleting table %s ", tableId);
    return new CleanUp(tableId, namespaceId);
  }

  @Override
  public void undo(long tid, Master environment) throws Exception {
    if (namespaceId != null) {
      Utils.unreserveNamespace(namespaceId, tid, false);
    }
    Utils.unreserveTable(tableId, tid, true);
  }
}
