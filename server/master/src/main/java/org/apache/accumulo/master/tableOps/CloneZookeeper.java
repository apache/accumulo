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

import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.impl.Namespaces;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.thrift.TableOperation;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.tables.TableManager;

class CloneZookeeper extends MasterRepo {

  private static final long serialVersionUID = 1L;

  private CloneInfo cloneInfo;

  public CloneZookeeper(CloneInfo cloneInfo) throws NamespaceNotFoundException {
    this.cloneInfo = cloneInfo;
    this.cloneInfo.namespaceId = Namespaces.getNamespaceId(HdfsZooInstance.getInstance(), Tables.qualify(this.cloneInfo.tableName).getFirst());
  }

  @Override
  public long isReady(long tid, Master environment) throws Exception {
    long val = 0;
    if (!cloneInfo.srcNamespaceId.equals(cloneInfo.namespaceId))
      val += Utils.reserveNamespace(cloneInfo.namespaceId, tid, false, true, TableOperation.CLONE);
    val += Utils.reserveTable(cloneInfo.tableId, tid, true, false, TableOperation.CLONE);
    return val;
  }

  @Override
  public Repo<Master> call(long tid, Master environment) throws Exception {
    Utils.tableNameLock.lock();
    try {
      // write tableName & tableId to zookeeper

      Utils.checkTableDoesNotExist(environment.getInstance(), cloneInfo.tableName, cloneInfo.tableId, TableOperation.CLONE);

      TableManager.getInstance().cloneTable(cloneInfo.srcTableId, cloneInfo.tableId, cloneInfo.tableName, cloneInfo.namespaceId, cloneInfo.propertiesToSet,
          cloneInfo.propertiesToExclude, NodeExistsPolicy.OVERWRITE);
      Tables.clearCache(environment.getInstance());

      return new CloneMetadata(cloneInfo);
    } finally {
      Utils.tableNameLock.unlock();
    }
  }

  @Override
  public void undo(long tid, Master environment) throws Exception {
    TableManager.getInstance().removeTable(cloneInfo.tableId);
    if (!cloneInfo.srcNamespaceId.equals(cloneInfo.namespaceId))
      Utils.unreserveNamespace(cloneInfo.namespaceId, tid, false);
    Utils.unreserveTable(cloneInfo.tableId, tid, true);
    Tables.clearCache(environment.getInstance());
  }

}
