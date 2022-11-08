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
package org.apache.accumulo.manager.tableOps.namespace.create;

import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.manager.tableOps.Utils;
import org.apache.accumulo.server.conf.store.NamespacePropKey;
import org.apache.accumulo.server.tables.TableManager;
import org.apache.accumulo.server.util.PropUtil;

class PopulateZookeeperWithNamespace extends ManagerRepo {

  private static final long serialVersionUID = 1L;

  private final NamespaceInfo namespaceInfo;

  PopulateZookeeperWithNamespace(NamespaceInfo ti) {
    this.namespaceInfo = ti;
  }

  @Override
  public long isReady(long id, Manager environment) throws Exception {
    return Utils.reserveNamespace(environment, namespaceInfo.namespaceId, id, true, false,
        TableOperation.CREATE);
  }

  @Override
  public Repo<Manager> call(long tid, Manager manager) throws Exception {

    Utils.getTableNameLock().lock();
    try {
      Utils.checkNamespaceDoesNotExist(manager.getContext(), namespaceInfo.namespaceName,
          namespaceInfo.namespaceId, TableOperation.CREATE);

      TableManager.prepareNewNamespaceState(manager.getContext(), namespaceInfo.namespaceId,
          namespaceInfo.namespaceName, NodeExistsPolicy.OVERWRITE);

      PropUtil.setProperties(manager.getContext(),
          NamespacePropKey.of(manager.getContext(), namespaceInfo.namespaceId),
          namespaceInfo.props);

      manager.getContext().clearTableListCache();

      return new FinishCreateNamespace(namespaceInfo);
    } finally {
      Utils.getTableNameLock().unlock();
    }
  }

  @Override
  public void undo(long tid, Manager manager) throws Exception {
    manager.getTableManager().removeNamespace(namespaceInfo.namespaceId);
    manager.getContext().clearTableListCache();
    Utils.unreserveNamespace(manager, namespaceInfo.namespaceId, tid, true);
  }

}
