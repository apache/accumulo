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
package org.apache.accumulo.manager.tableOps.clone;

import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.fate.zookeeper.DistributedReadWriteLock.LockType;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.manager.tableOps.Utils;

class CloneZookeeper extends ManagerRepo {

  private static final long serialVersionUID = 1L;

  private final CloneInfo cloneInfo;

  public CloneZookeeper(CloneInfo cloneInfo, ClientContext context)
      throws NamespaceNotFoundException {
    this.cloneInfo = cloneInfo;
  }

  @Override
  public long isReady(FateId fateId, Manager environment) throws Exception {
    long val = 0;
    if (!cloneInfo.getSrcNamespaceId().equals(cloneInfo.getNamespaceId())) {
      val += Utils.reserveNamespace(environment, cloneInfo.getNamespaceId(), fateId, LockType.READ,
          true, TableOperation.CLONE);
    }
    val += Utils.reserveTable(environment, cloneInfo.getTableId(), fateId, LockType.WRITE, false,
        TableOperation.CLONE);
    return val;
  }

  @Override
  public Repo<Manager> call(FateId fateId, Manager environment) throws Exception {
    Utils.getTableNameLock().lock();
    try {
      var context = environment.getContext();
      // write tableName & tableId, first to Table Mapping and then to Zookeeper
      context.getTableMapping(cloneInfo.getNamespaceId()).put(cloneInfo.getTableId(),
          cloneInfo.getTableName(), TableOperation.CLONE);
      environment.getTableManager().cloneTable(cloneInfo.getSrcTableId(), cloneInfo.getTableId(),
          cloneInfo.getTableName(), cloneInfo.getNamespaceId(), cloneInfo.getPropertiesToSet(),
          cloneInfo.getPropertiesToExclude());
      context.clearTableListCache();

      return new CloneMetadata(cloneInfo);
    } finally {
      Utils.getTableNameLock().unlock();
    }
  }

  @Override
  public void undo(FateId fateId, Manager environment) throws Exception {
    environment.getTableManager().removeTable(cloneInfo.getTableId(), cloneInfo.getNamespaceId());
    if (!cloneInfo.getSrcNamespaceId().equals(cloneInfo.getNamespaceId())) {
      Utils.unreserveNamespace(environment, cloneInfo.getNamespaceId(), fateId, LockType.READ);
    }
    Utils.unreserveTable(environment, cloneInfo.getTableId(), fateId, LockType.WRITE);
    environment.getContext().clearTableListCache();
  }

}
