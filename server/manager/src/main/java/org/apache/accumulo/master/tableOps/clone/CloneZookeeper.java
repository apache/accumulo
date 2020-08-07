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
package org.apache.accumulo.master.tableOps.clone;

import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.Namespaces;
import org.apache.accumulo.core.clientImpl.Tables;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.master.tableOps.MasterRepo;
import org.apache.accumulo.master.tableOps.Utils;

class CloneZookeeper extends MasterRepo {

  private static final long serialVersionUID = 1L;

  private CloneInfo cloneInfo;

  public CloneZookeeper(CloneInfo cloneInfo, ClientContext context)
      throws NamespaceNotFoundException {
    this.cloneInfo = cloneInfo;
    this.cloneInfo.namespaceId =
        Namespaces.getNamespaceId(context, Tables.qualify(this.cloneInfo.tableName).getFirst());
  }

  @Override
  public long isReady(long tid, Master environment) throws Exception {
    long val = 0;
    if (!cloneInfo.srcNamespaceId.equals(cloneInfo.namespaceId))
      val += Utils.reserveNamespace(environment, cloneInfo.namespaceId, tid, false, true,
          TableOperation.CLONE);
    val +=
        Utils.reserveTable(environment, cloneInfo.tableId, tid, true, false, TableOperation.CLONE);
    return val;
  }

  @Override
  public Repo<Master> call(long tid, Master environment) throws Exception {
    Utils.getTableNameLock().lock();
    try {
      // write tableName & tableId to zookeeper

      Utils.checkTableDoesNotExist(environment.getContext(), cloneInfo.tableName, cloneInfo.tableId,
          TableOperation.CLONE);

      environment.getTableManager().cloneTable(cloneInfo.srcTableId, cloneInfo.tableId,
          cloneInfo.tableName, cloneInfo.namespaceId, cloneInfo.propertiesToSet,
          cloneInfo.propertiesToExclude, NodeExistsPolicy.OVERWRITE);
      Tables.clearCache(environment.getContext());

      return new CloneMetadata(cloneInfo);
    } finally {
      Utils.getTableNameLock().unlock();
    }
  }

  @Override
  public void undo(long tid, Master environment) throws Exception {
    environment.getTableManager().removeTable(cloneInfo.tableId);
    if (!cloneInfo.srcNamespaceId.equals(cloneInfo.namespaceId))
      Utils.unreserveNamespace(environment, cloneInfo.namespaceId, tid, false);
    Utils.unreserveTable(environment, cloneInfo.tableId, tid, true);
    Tables.clearCache(environment.getContext());
  }

}
