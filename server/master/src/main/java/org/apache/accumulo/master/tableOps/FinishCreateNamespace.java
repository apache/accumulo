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

import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.master.Master;
import org.slf4j.LoggerFactory;

class FinishCreateNamespace extends MasterRepo {

  private static final long serialVersionUID = 1L;

  private NamespaceInfo namespaceInfo;

  public FinishCreateNamespace(NamespaceInfo ti) {
    this.namespaceInfo = ti;
  }

  @Override
  public long isReady(long tid, Master environment) throws Exception {
    return 0;
  }

  @Override
  public Repo<Master> call(long id, Master env) throws Exception {

    Utils.unreserveNamespace(namespaceInfo.namespaceId, id, true);

    env.getEventCoordinator().event("Created namespace %s ", namespaceInfo.namespaceName);

    LoggerFactory.getLogger(FinishCreateNamespace.class).debug("Created table " + namespaceInfo.namespaceId + " " + namespaceInfo.namespaceName);

    return null;
  }

  @Override
  public String getReturn() {
    return namespaceInfo.namespaceId.canonicalID();
  }

  @Override
  public void undo(long tid, Master env) throws Exception {}

}
