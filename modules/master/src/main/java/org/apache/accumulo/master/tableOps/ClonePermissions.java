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
import org.apache.accumulo.core.client.impl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.client.impl.thrift.TableOperation;
import org.apache.accumulo.core.client.impl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.security.AuditedSecurityOperation;
import org.slf4j.LoggerFactory;

class ClonePermissions extends MasterRepo {

  private static final long serialVersionUID = 1L;

  private CloneInfo cloneInfo;

  public ClonePermissions(CloneInfo cloneInfo) {
    this.cloneInfo = cloneInfo;
  }

  @Override
  public long isReady(long tid, Master environment) throws Exception {
    return 0;
  }

  @Override
  public Repo<Master> call(long tid, Master environment) throws Exception {
    // give all table permissions to the creator
    for (TablePermission permission : TablePermission.values()) {
      try {
        AuditedSecurityOperation.getInstance(environment).grantTablePermission(environment.rpcCreds(), cloneInfo.user, cloneInfo.tableId, permission,
            cloneInfo.namespaceId);
      } catch (ThriftSecurityException e) {
        LoggerFactory.getLogger(ClonePermissions.class).error("{}", e.getMessage(), e);
        throw e;
      }
    }

    // setup permissions in zookeeper before table info in zookeeper
    // this way concurrent users will not get a spurious pemission denied
    // error
    try {
      return new CloneZookeeper(cloneInfo);
    } catch (NamespaceNotFoundException e) {
      throw new AcceptableThriftTableOperationException(null, cloneInfo.tableName, TableOperation.CLONE, TableOperationExceptionType.NAMESPACE_NOTFOUND,
          "Namespace for target table not found");
    }
  }

  @Override
  public void undo(long tid, Master environment) throws Exception {
    AuditedSecurityOperation.getInstance(environment).deleteTable(environment.rpcCreds(), cloneInfo.tableId, cloneInfo.namespaceId);
  }
}
