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

import static java.nio.charset.StandardCharsets.UTF_8;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.clientImpl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.clientImpl.Namespaces;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.clientImpl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.tables.TableNameUtil;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.manager.tableOps.Utils;
import org.slf4j.LoggerFactory;

public class RenameTable extends ManagerRepo {

  private static final long serialVersionUID = 1L;
  private TableId tableId;
  private NamespaceId namespaceId;
  private String oldTableName;
  private String newTableName;

  @Override
  public long isReady(long tid, Manager env) throws Exception {
    return Utils.reserveNamespace(env, namespaceId, tid, false, true, TableOperation.RENAME)
        + Utils.reserveTable(env, tableId, tid, true, true, TableOperation.RENAME);
  }

  public RenameTable(NamespaceId namespaceId, TableId tableId, String oldTableName,
      String newTableName) throws NamespaceNotFoundException {
    this.namespaceId = namespaceId;
    this.tableId = tableId;
    this.oldTableName = oldTableName;
    this.newTableName = newTableName;
  }

  @Override
  public Repo<Manager> call(long tid, Manager manager) throws Exception {
    Pair<String,String> qualifiedOldTableName = TableNameUtil.qualify(oldTableName);
    Pair<String,String> qualifiedNewTableName = TableNameUtil.qualify(newTableName);

    // ensure no attempt is made to rename across namespaces
    if (newTableName.contains(".") && !namespaceId.equals(
        Namespaces.getNamespaceId(manager.getContext(), qualifiedNewTableName.getFirst()))) {
      throw new AcceptableThriftTableOperationException(tableId.canonical(), oldTableName,
          TableOperation.RENAME, TableOperationExceptionType.INVALID_NAME,
          "Namespace in new table name does not match the old table name");
    }

    ZooReaderWriter zoo = manager.getContext().getZooReaderWriter();

    Utils.getTableNameLock().lock();
    try {
      Utils.checkTableDoesNotExist(manager.getContext(), newTableName, tableId,
          TableOperation.RENAME);

      final String newName = qualifiedNewTableName.getSecond();
      final String oldName = qualifiedOldTableName.getSecond();

      final String tap =
          manager.getZooKeeperRoot() + Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_NAME;

      zoo.mutateExisting(tap, current -> {
        final String currentName = new String(current, UTF_8);
        if (currentName.equals(newName)) {
          return null; // assume in this case the operation is running again, so we are done
        }
        if (!currentName.equals(oldName)) {
          throw new AcceptableThriftTableOperationException(null, oldTableName,
              TableOperation.RENAME, TableOperationExceptionType.NOTFOUND,
              "Name changed while processing");
        }
        return newName.getBytes(UTF_8);
      });
      manager.getContext().clearTableListCache();
    } finally {
      Utils.getTableNameLock().unlock();
      Utils.unreserveTable(manager, tableId, tid, true);
      Utils.unreserveNamespace(manager, namespaceId, tid, false);
    }

    LoggerFactory.getLogger(RenameTable.class).debug("Renamed table {} {} {}", tableId,
        oldTableName, newTableName);

    return null;
  }

  @Override
  public void undo(long tid, Manager env) {
    Utils.unreserveTable(env, tableId, tid, true);
    Utils.unreserveNamespace(env, namespaceId, tid, false);
  }

}
