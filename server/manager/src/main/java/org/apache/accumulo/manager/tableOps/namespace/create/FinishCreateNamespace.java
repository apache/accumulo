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
package org.apache.accumulo.manager.tableOps.namespace.create;

import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.manager.tableOps.Utils;
import org.slf4j.LoggerFactory;

class FinishCreateNamespace extends ManagerRepo {

  private static final long serialVersionUID = 1L;

  private NamespaceInfo namespaceInfo;

  public FinishCreateNamespace(NamespaceInfo ti) {
    this.namespaceInfo = ti;
  }

  @Override
  public long isReady(long tid, Manager environment) {
    return 0;
  }

  @Override
  public Repo<Manager> call(long id, Manager env) {

    Utils.unreserveNamespace(env, namespaceInfo.namespaceId, id, true);

    env.getEventCoordinator().event("Created namespace %s ", namespaceInfo.namespaceName);

    LoggerFactory.getLogger(FinishCreateNamespace.class)
        .debug("Created table " + namespaceInfo.namespaceId + " " + namespaceInfo.namespaceName);

    return null;
  }

  @Override
  public String getReturn() {
    return namespaceInfo.namespaceId.canonical();
  }

  @Override
  public void undo(long tid, Manager env) {}

}
