/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.master.tableOps.namespace.create;

import java.util.Map.Entry;

import org.apache.accumulo.core.clientImpl.Tables;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.master.tableOps.MasterRepo;
import org.apache.accumulo.master.tableOps.Utils;
import org.apache.accumulo.server.tables.TableManager;
import org.apache.accumulo.server.util.NamespacePropUtil;

class PopulateZookeeperWithNamespace extends MasterRepo {

  private static final long serialVersionUID = 1L;

  private NamespaceInfo namespaceInfo;

  PopulateZookeeperWithNamespace(NamespaceInfo ti) {
    this.namespaceInfo = ti;
  }

  @Override
  public long isReady(long id, Master environment) throws Exception {
    return Utils.reserveNamespace(environment, namespaceInfo.namespaceId, id, true, false,
        TableOperation.CREATE);
  }

  @Override
  public Repo<Master> call(long tid, Master master) throws Exception {

    Utils.getTableNameLock().lock();
    try {
      Utils.checkNamespaceDoesNotExist(master.getContext(), namespaceInfo.namespaceName,
          namespaceInfo.namespaceId, TableOperation.CREATE);

      TableManager.prepareNewNamespaceState(master.getContext().getZooReaderWriter(),
          master.getInstanceID(), namespaceInfo.namespaceId, namespaceInfo.namespaceName,
          NodeExistsPolicy.OVERWRITE);

      for (Entry<String,String> entry : namespaceInfo.props.entrySet())
        NamespacePropUtil.setNamespaceProperty(master.getContext(), namespaceInfo.namespaceId,
            entry.getKey(), entry.getValue());

      Tables.clearCache(master.getContext());

      return new FinishCreateNamespace(namespaceInfo);
    } finally {
      Utils.getTableNameLock().unlock();
    }
  }

  @Override
  public void undo(long tid, Master master) throws Exception {
    master.getTableManager().removeNamespace(namespaceInfo.namespaceId);
    Tables.clearCache(master.getContext());
    Utils.unreserveNamespace(master, namespaceInfo.namespaceId, tid, true);
  }

}
