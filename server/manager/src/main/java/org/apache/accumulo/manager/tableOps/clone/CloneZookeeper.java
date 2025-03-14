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
import org.apache.accumulo.core.clientImpl.Namespaces;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.util.tables.TableNameUtil;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.manager.tableOps.Utils;

class CloneZookeeper extends ManagerRepo {

  private static final long serialVersionUID = 1L;

  private final CloneInfo cloneInfo;

  public CloneZookeeper(CloneInfo cloneInfo, ClientContext context)
      throws NamespaceNotFoundException {
    this.cloneInfo = cloneInfo;
    if (this.cloneInfo.getNamespaceId() == null) {
      // Prior to 2.1.4 the namespaceId was calculated in this
      // step and set on the cloneInfo object here. If for some
      // reason we are processing a pre-2.1.3 CloneTable operation,
      // then we need to continue to set this here as it will be
      // null in the deserialized CloneInfo object.
      //
      // TODO: Remove this check in 3.1 as Fate operations
      // need to be cleaned up before a major upgrade.
      this.cloneInfo.setNamespaceId(Namespaces.getNamespaceId(context,
          TableNameUtil.qualify(this.cloneInfo.getTableName()).getFirst()));
    }
  }

  @Override
  public long isReady(long tid, Manager environment) throws Exception {
    return Utils.reserveTable(environment, cloneInfo.getTableId(), tid, true, false,
        TableOperation.CLONE);
  }

  @Override
  public Repo<Manager> call(long tid, Manager environment) throws Exception {
    Utils.getTableNameLock().lock();
    try {
      // write tableName & tableId to zookeeper

      Utils.checkTableNameDoesNotExist(environment.getContext(), cloneInfo.getTableName(),
          cloneInfo.getNamespaceId(), cloneInfo.getTableId(), TableOperation.CLONE);

      environment.getTableManager().cloneTable(cloneInfo.getSrcTableId(), cloneInfo.getTableId(),
          cloneInfo.getTableName(), cloneInfo.getNamespaceId(), cloneInfo.getPropertiesToSet(),
          cloneInfo.getPropertiesToExclude());
      environment.getContext().clearTableListCache();

      return new CloneMetadata(cloneInfo);
    } finally {
      Utils.getTableNameLock().unlock();
    }
  }

  @Override
  public void undo(long tid, Manager environment) throws Exception {
    environment.getTableManager().removeTable(cloneInfo.getTableId());
    Utils.unreserveTable(environment, cloneInfo.getTableId(), tid, true);
    environment.getContext().clearTableListCache();
  }

}
