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
package org.apache.accumulo.manager.tableOps.create;

import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.manager.tableOps.TableInfo;
import org.apache.accumulo.server.security.SecurityOperation;
import org.slf4j.LoggerFactory;

class SetupPermissions extends ManagerRepo {

  private static final long serialVersionUID = 1L;

  private TableInfo tableInfo;

  SetupPermissions(TableInfo ti) {
    this.tableInfo = ti;
  }

  @Override
  public Repo<Manager> call(long tid, Manager env) throws Exception {
    // give all table permissions to the creator
    SecurityOperation security = env.getContext().getSecurityOperation();
    if (!tableInfo.getUser().equals(env.getContext().getCredentials().getPrincipal())) {
      for (TablePermission permission : TablePermission.values()) {
        try {
          security.grantTablePermission(env.getContext().rpcCreds(), tableInfo.getUser(),
              tableInfo.getTableId(), permission, tableInfo.getNamespaceId());
        } catch (ThriftSecurityException e) {
          LoggerFactory.getLogger(SetupPermissions.class).error("{}", e.getMessage(), e);
          throw e;
        }
      }
    }

    // setup permissions in zookeeper before table info in zookeeper
    // this way concurrent users will not get a spurious permission denied
    // error
    return new PopulateZookeeper(tableInfo);
  }

  @Override
  public void undo(long tid, Manager env) throws Exception {
    env.getContext().getSecurityOperation().deleteTable(env.getContext().rpcCreds(),
        tableInfo.getTableId(), tableInfo.getNamespaceId());
  }

}
