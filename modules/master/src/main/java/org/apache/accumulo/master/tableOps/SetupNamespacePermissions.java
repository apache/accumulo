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

import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.security.NamespacePermission;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.security.AuditedSecurityOperation;
import org.apache.accumulo.server.security.SecurityOperation;
import org.slf4j.LoggerFactory;

class SetupNamespacePermissions extends MasterRepo {

  private static final long serialVersionUID = 1L;

  private NamespaceInfo namespaceInfo;

  public SetupNamespacePermissions(NamespaceInfo ti) {
    this.namespaceInfo = ti;
  }

  @Override
  public Repo<Master> call(long tid, Master env) throws Exception {
    // give all namespace permissions to the creator
    SecurityOperation security = AuditedSecurityOperation.getInstance(env);
    for (NamespacePermission permission : NamespacePermission.values()) {
      try {
        security.grantNamespacePermission(env.rpcCreds(), namespaceInfo.user, namespaceInfo.namespaceId, permission);
      } catch (ThriftSecurityException e) {
        LoggerFactory.getLogger(SetupNamespacePermissions.class).error("{}", e.getMessage(), e);
        throw e;
      }
    }

    // setup permissions in zookeeper before table info in zookeeper
    // this way concurrent users will not get a spurious permission denied
    // error
    return new PopulateZookeeperWithNamespace(namespaceInfo);
  }
}
