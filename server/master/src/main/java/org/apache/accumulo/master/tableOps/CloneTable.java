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

import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.thrift.TableOperation;
import org.apache.accumulo.core.client.impl.thrift.TableOperationExceptionType;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.client.HdfsZooInstance;

public class CloneTable extends MasterRepo {

  private static final long serialVersionUID = 1L;
  private CloneInfo cloneInfo;

  public CloneTable(String user, String srcTableId, String tableName, Map<String,String> propertiesToSet, Set<String> propertiesToExclude)
      throws AcceptableThriftTableOperationException {
    cloneInfo = new CloneInfo();
    cloneInfo.user = user;
    cloneInfo.srcTableId = srcTableId;
    cloneInfo.tableName = tableName;
    cloneInfo.propertiesToExclude = propertiesToExclude;
    cloneInfo.propertiesToSet = propertiesToSet;
    Instance inst = HdfsZooInstance.getInstance();
    try {
      cloneInfo.srcNamespaceId = Tables.getNamespaceId(inst, cloneInfo.srcTableId);
    } catch (IllegalArgumentException e) {
      if (inst == null || cloneInfo.srcTableId == null) {
        // just throw the exception if the illegal argument was thrown by the argument checker and not due to table non-existence
        throw e;
      }
      throw new AcceptableThriftTableOperationException(cloneInfo.srcTableId, "", TableOperation.CLONE, TableOperationExceptionType.NOTFOUND,
          "Table does not exist");
    }
  }

  @Override
  public long isReady(long tid, Master environment) throws Exception {
    long val = Utils.reserveNamespace(cloneInfo.srcNamespaceId, tid, false, true, TableOperation.CLONE);
    val += Utils.reserveTable(cloneInfo.srcTableId, tid, false, true, TableOperation.CLONE);
    return val;
  }

  @Override
  public Repo<Master> call(long tid, Master environment) throws Exception {

    Utils.idLock.lock();
    try {
      cloneInfo.tableId = Utils.getNextTableId(cloneInfo.tableName, environment.getInstance());
      return new ClonePermissions(cloneInfo);
    } finally {
      Utils.idLock.unlock();
    }
  }

  @Override
  public void undo(long tid, Master environment) throws Exception {
    Utils.unreserveNamespace(cloneInfo.srcNamespaceId, tid, false);
    Utils.unreserveTable(cloneInfo.srcTableId, tid, false);
  }

}
