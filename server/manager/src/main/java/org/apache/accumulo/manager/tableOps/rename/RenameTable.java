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
package org.apache.accumulo.manager.tableOps.rename;

import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.clientImpl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.clientImpl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.fate.zookeeper.DistributedReadWriteLock.LockType;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.tables.TableNameUtil;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.manager.tableOps.Utils;
import org.slf4j.LoggerFactory;

public class RenameTable extends ManagerRepo {

  private static final long serialVersionUID = 1L;
  private final TableId tableId;
  private final NamespaceId namespaceId;
  private final String oldTableName;
  private final String newTableName;

  @Override
  public long isReady(FateId fateId, Manager env) throws Exception {
    return Utils.reserveNamespace(env, namespaceId, fateId, LockType.READ, true,
        TableOperation.RENAME)
        + Utils.reserveTable(env, tableId, fateId, LockType.WRITE, true, TableOperation.RENAME);
  }

  public RenameTable(NamespaceId namespaceId, TableId tableId, String oldTableName,
      String newTableName) throws NamespaceNotFoundException {
    this.namespaceId = namespaceId;
    this.tableId = tableId;
    this.oldTableName = oldTableName;
    this.newTableName = newTableName;
  }

  @Override
  public Repo<Manager> call(FateId fateId, Manager manager) throws Exception {
    Pair<String,String> qualifiedOldTableName = TableNameUtil.qualify(oldTableName);
    Pair<String,String> qualifiedNewTableName = TableNameUtil.qualify(newTableName);
    var context = manager.getContext();

    // ensure no attempt is made to rename across namespaces
    if (newTableName.contains(".")
        && !context.getNamespaceId(qualifiedNewTableName.getFirst()).equals(namespaceId)) {
      throw new AcceptableThriftTableOperationException(tableId.canonical(), oldTableName,
          TableOperation.RENAME, TableOperationExceptionType.INVALID_NAME,
          "Namespace in new table name does not match the old table name");
    }

    Utils.getTableNameLock().lock();
    try {
      context.getTableMapping(namespaceId).rename(tableId, qualifiedOldTableName.getSecond(),
          qualifiedNewTableName.getSecond());

      context.clearTableListCache();
    } finally {
      Utils.getTableNameLock().unlock();
      Utils.unreserveTable(manager, tableId, fateId, LockType.WRITE);
      Utils.unreserveNamespace(manager, namespaceId, fateId, LockType.READ);
    }

    LoggerFactory.getLogger(RenameTable.class).debug("Renamed table {} {} {}", tableId,
        oldTableName, newTableName);

    return null;
  }

  @Override
  public void undo(FateId fateId, Manager env) {
    Utils.unreserveTable(env, tableId, fateId, LockType.WRITE);
    Utils.unreserveNamespace(env, namespaceId, fateId, LockType.READ);
  }

}
