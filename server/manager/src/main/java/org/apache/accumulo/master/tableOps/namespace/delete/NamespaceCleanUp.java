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
package org.apache.accumulo.master.tableOps.namespace.delete;

import org.apache.accumulo.core.clientImpl.Tables;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.master.tableOps.MasterRepo;
import org.apache.accumulo.master.tableOps.Utils;
import org.apache.accumulo.server.security.AuditedSecurityOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class NamespaceCleanUp extends MasterRepo {

  private static final Logger log = LoggerFactory.getLogger(NamespaceCleanUp.class);

  private static final long serialVersionUID = 1L;

  private NamespaceId namespaceId;

  public NamespaceCleanUp(NamespaceId namespaceId) {
    this.namespaceId = namespaceId;
  }

  @Override
  public long isReady(long tid, Master master) {
    return 0;
  }

  @Override
  public Repo<Master> call(long id, Master master) {

    // remove from zookeeper
    try {
      master.getTableManager().removeNamespace(namespaceId);
    } catch (Exception e) {
      log.error("Failed to find namespace in zookeeper", e);
    }
    Tables.clearCache(master.getContext());

    // remove any permissions associated with this namespace
    try {
      AuditedSecurityOperation.getInstance(master.getContext())
          .deleteNamespace(master.getContext().rpcCreds(), namespaceId);
    } catch (ThriftSecurityException e) {
      log.error("{}", e.getMessage(), e);
    }

    Utils.unreserveNamespace(master, namespaceId, id, true);

    log.debug("Deleted namespace " + namespaceId);

    return null;
  }

  @Override
  public void undo(long tid, Master environment) {
    // nothing to do
  }

}
