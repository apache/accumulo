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

import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.master.tableOps.MasterRepo;
import org.apache.accumulo.master.tableOps.Utils;

public class CloneTable extends MasterRepo {

  private static final long serialVersionUID = 1L;
  private CloneInfo cloneInfo;

  public CloneTable(String user, NamespaceId namespaceId, TableId srcTableId, String tableName,
      Map<String,String> propertiesToSet, Set<String> propertiesToExclude, boolean keepOffline) {
    cloneInfo = new CloneInfo();
    cloneInfo.user = user;
    cloneInfo.srcTableId = srcTableId;
    cloneInfo.tableName = tableName;
    cloneInfo.propertiesToExclude = propertiesToExclude;
    cloneInfo.propertiesToSet = propertiesToSet;
    cloneInfo.srcNamespaceId = namespaceId;
    cloneInfo.keepOffline = keepOffline;
  }

  @Override
  public long isReady(long tid, Master environment) throws Exception {
    long val = Utils.reserveNamespace(environment, cloneInfo.srcNamespaceId, tid, false, true,
        TableOperation.CLONE);
    val += Utils.reserveTable(environment, cloneInfo.srcTableId, tid, false, true,
        TableOperation.CLONE);
    return val;
  }

  @Override
  public Repo<Master> call(long tid, Master environment) throws Exception {

    Utils.getIdLock().lock();
    try {
      cloneInfo.tableId =
          Utils.getNextId(cloneInfo.tableName, environment.getContext(), TableId::of);

      return new ClonePermissions(cloneInfo);
    } finally {
      Utils.getIdLock().unlock();
    }
  }

  @Override
  public void undo(long tid, Master environment) {
    Utils.unreserveNamespace(environment, cloneInfo.srcNamespaceId, tid, false);
    Utils.unreserveTable(environment, cloneInfo.srcTableId, tid, false);
  }

}
