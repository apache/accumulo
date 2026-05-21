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

import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.fate.zookeeper.DistributedReadWriteLock.LockType;
import org.apache.accumulo.manager.tableOps.AbstractFateOperation;
import org.apache.accumulo.manager.tableOps.FateEnv;
import org.apache.accumulo.manager.tableOps.Utils;

public class CloneTable extends AbstractFateOperation {

  private static final long serialVersionUID = 1L;
  private final CloneInfo cloneInfo;

  public CloneTable(String user, NamespaceId srcNamespaceId, TableId srcTableId,
      NamespaceId namespaceId, String tableName, Map<String,String> propertiesToSet,
      Set<String> propertiesToExclude, boolean keepOffline) {
    cloneInfo = new CloneInfo(srcNamespaceId, srcTableId, namespaceId, tableName, propertiesToSet,
        propertiesToExclude, keepOffline, user);
  }

  @Override
  public long isReady(FateId fateId, FateEnv environment) throws Exception {
    long val = Utils.reserveNamespace(environment.getContext(), cloneInfo.getNamespaceId(), fateId,
        LockType.READ, true, TableOperation.CLONE);
    val += Utils.reserveTable(environment.getContext(), cloneInfo.getSrcTableId(), fateId,
        LockType.READ, true, TableOperation.CLONE);
    return val;
  }

  @Override
  public Repo<FateEnv> call(FateId fateId, FateEnv environment) throws Exception {

    cloneInfo.setTableId(
        Utils.getNextId(cloneInfo.getTableName(), environment.getContext(), TableId::of));
    return new ClonePermissions(cloneInfo);
  }

  @Override
  public void undo(FateId fateId, FateEnv environment) {
    Utils.unreserveNamespace(environment.getContext(), cloneInfo.getNamespaceId(), fateId,
        LockType.READ);
    Utils.unreserveTable(environment.getContext(), cloneInfo.getSrcTableId(), fateId,
        LockType.READ);
  }

}
