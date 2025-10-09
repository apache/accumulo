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
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.fate.zookeeper.DistributedReadWriteLock.LockType;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.manager.tableOps.AbstractFateOperation;
import org.apache.accumulo.manager.tableOps.FateEnv;
import org.apache.accumulo.manager.tableOps.Utils;
import org.apache.accumulo.server.conf.store.NamespacePropKey;
import org.apache.accumulo.server.util.PropUtil;

class PopulateZookeeperWithNamespace extends AbstractFateOperation {

  private static final long serialVersionUID = 1L;

  private final NamespaceInfo namespaceInfo;

  PopulateZookeeperWithNamespace(NamespaceInfo ti) {
    this.namespaceInfo = ti;
  }

  @Override
  public long isReady(FateId fateId, FateEnv environment) throws Exception {
    return Utils.reserveNamespace(environment.getContext(), namespaceInfo.namespaceId, fateId,
        LockType.WRITE, false, TableOperation.CREATE);
  }

  @Override
  public Repo<FateEnv> call(FateId fateId, FateEnv env) throws Exception {

    var context = env.getContext();
    context.getNamespaceMapping().put(namespaceInfo.namespaceId, namespaceInfo.namespaceName);
    context.getTableManager().prepareNewNamespaceState(namespaceInfo.namespaceId,
        namespaceInfo.namespaceName, NodeExistsPolicy.OVERWRITE);

    PropUtil.setProperties(context, NamespacePropKey.of(namespaceInfo.namespaceId),
        namespaceInfo.props);

    context.clearTableListCache();

    return new FinishCreateNamespace(namespaceInfo);
  }

  @Override
  public void undo(FateId fateId, FateEnv env) throws Exception {
    env.getTableManager().removeNamespace(namespaceInfo.namespaceId);
    env.getContext().clearTableListCache();
    Utils.unreserveNamespace(env.getContext(), namespaceInfo.namespaceId, fateId, LockType.WRITE);
  }

}
