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
package org.apache.accumulo.manager.tableOps.namespace.delete;

import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.manager.tableOps.Utils;

public class DeleteNamespace extends ManagerRepo {

  private static final long serialVersionUID = 1L;

  private NamespaceId namespaceId;

  public DeleteNamespace(NamespaceId namespaceId) {
    this.namespaceId = namespaceId;
  }

  @Override
  public long isReady(long id, Manager environment) throws Exception {
    return Utils.reserveNamespace(environment, namespaceId, id, true, true, TableOperation.DELETE);
  }

  @Override
  public Repo<Manager> call(long tid, Manager environment) {
    environment.getEventCoordinator().event("deleting namespace %s ", namespaceId);
    return new NamespaceCleanUp(namespaceId);
  }

  @Override
  public void undo(long id, Manager environment) {
    Utils.unreserveNamespace(environment, namespaceId, id, true);
  }

}
