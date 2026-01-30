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
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.fate.zookeeper.DistributedReadWriteLock.LockType;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.tables.TableNameUtil;
import org.apache.accumulo.manager.tableOps.AbstractFateOperation;
import org.apache.accumulo.manager.tableOps.FateEnv;
import org.apache.accumulo.manager.tableOps.Utils;
import org.slf4j.LoggerFactory;

public class RenameTable extends AbstractFateOperation {

  private static final long serialVersionUID = 1L;
  private final TableId tableId;
  private final NamespaceId namespaceId;
  private final String oldTableName;
  private final String newTableName;

  @Override
  public long isReady(FateId fateId, FateEnv env) throws Exception {
    return Utils.reserveNamespace(env.getContext(), namespaceId, fateId, LockType.READ, true,
        TableOperation.RENAME)
        + Utils.reserveTable(env.getContext(), tableId, fateId, LockType.WRITE, true,
            TableOperation.RENAME);
  }

  public RenameTable(NamespaceId namespaceId, TableId tableId, String oldTableName,
      String newTableName) throws NamespaceNotFoundException {
    this.namespaceId = namespaceId;
    this.tableId = tableId;
    this.oldTableName = oldTableName;
    this.newTableName = newTableName;
  }

  @Override
  public Repo<FateEnv> call(FateId fateId, FateEnv env) throws Exception {
    Pair<String,String> qualifiedOldTableName = TableNameUtil.qualify(oldTableName);
    // the namespace name was already checked before starting the fate operation
    String newSimpleTableName = TableNameUtil.qualify(newTableName).getSecond();
    var context = env.getContext();

    try {
      context.getTableMapping(namespaceId).rename(tableId, qualifiedOldTableName.getSecond(),
          newSimpleTableName);

      context.clearTableListCache();
    } finally {
      Utils.unreserveTable(context, tableId, fateId, LockType.WRITE);
      Utils.unreserveNamespace(context, namespaceId, fateId, LockType.READ);
    }

    LoggerFactory.getLogger(RenameTable.class).debug("Renamed table {} {} {}", tableId,
        oldTableName, newTableName);

    return null;
  }

  @Override
  public void undo(FateId fateId, FateEnv env) {
    Utils.unreserveTable(env.getContext(), tableId, fateId, LockType.WRITE);
    Utils.unreserveNamespace(env.getContext(), namespaceId, fateId, LockType.READ);
  }

}
