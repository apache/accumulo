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

import java.util.Map.Entry;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.thrift.TableOperation;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.master.Master;
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
    return Utils.reserveNamespace(namespaceInfo.namespaceId, id, true, false, TableOperation.CREATE);
  }

  @Override
  public Repo<Master> call(long tid, Master master) throws Exception {

    Utils.tableNameLock.lock();
    try {
      Instance instance = master.getInstance();

      Utils.checkNamespaceDoesNotExist(instance, namespaceInfo.namespaceName, namespaceInfo.namespaceId, TableOperation.CREATE);

      TableManager.prepareNewNamespaceState(instance.getInstanceID(), namespaceInfo.namespaceId, namespaceInfo.namespaceName, NodeExistsPolicy.OVERWRITE);

      for (Entry<String,String> entry : namespaceInfo.props.entrySet())
        NamespacePropUtil.setNamespaceProperty(namespaceInfo.namespaceId, entry.getKey(), entry.getValue());

      Tables.clearCache(instance);

      return new FinishCreateNamespace(namespaceInfo);
    } finally {
      Utils.tableNameLock.unlock();
    }
  }

  @Override
  public void undo(long tid, Master master) throws Exception {
    TableManager.getInstance().removeNamespace(namespaceInfo.namespaceId);
    Tables.clearCache(master.getInstance());
    Utils.unreserveNamespace(namespaceInfo.namespaceId, tid, true);
  }

}
