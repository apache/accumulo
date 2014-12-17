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

import java.io.Serializable;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.thrift.TableOperation;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.security.NamespacePermission;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.security.AuditedSecurityOperation;
import org.apache.accumulo.server.security.SecurityOperation;
import org.apache.accumulo.server.security.SystemCredentials;
import org.apache.accumulo.server.tables.TableManager;
import org.apache.accumulo.server.util.NamespacePropUtil;
import org.apache.log4j.Logger;

class NamespaceInfo implements Serializable {

  private static final long serialVersionUID = 1L;

  String namespaceName;
  String namespaceId;
  String user;

  public Map<String,String> props;
}

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

    Logger.getLogger(FinishCreateNamespace.class).debug("Created table " + namespaceInfo.namespaceId + " " + namespaceInfo.namespaceName);

    return null;
  }

  @Override
  public String getReturn() {
    return namespaceInfo.namespaceId;
  }

  @Override
  public void undo(long tid, Master env) throws Exception {}

}

class PopulateZookeeperWithNamespace extends MasterRepo {

  private static final long serialVersionUID = 1L;

  private NamespaceInfo namespaceInfo;

  PopulateZookeeperWithNamespace(NamespaceInfo ti) {
    this.namespaceInfo = ti;
  }

  @Override
  public long isReady(long id, Master environment) throws Exception {
    return Utils.reserveNamespace(namespaceInfo.namespaceId, id, true, false, TableOperation.CREATE);
  }

  @Override
  public Repo<Master> call(long tid, Master master) throws Exception {

    Utils.tableNameLock.lock();
    try {
      Instance instance = master.getInstance();

      Utils.checkNamespaceDoesNotExist(instance, namespaceInfo.namespaceName, namespaceInfo.namespaceId, TableOperation.CREATE);

      TableManager.prepareNewNamespaceState(instance.getInstanceID(), namespaceInfo.namespaceId, namespaceInfo.namespaceName, NodeExistsPolicy.OVERWRITE);

      for (Entry<String,String> entry : namespaceInfo.props.entrySet())
        NamespacePropUtil.setNamespaceProperty(namespaceInfo.namespaceId, entry.getKey(), entry.getValue());

      Tables.clearCache(instance);

      return new FinishCreateNamespace(namespaceInfo);
    } finally {
      Utils.tableNameLock.unlock();
    }
  }

  @Override
  public void undo(long tid, Master master) throws Exception {
    TableManager.getInstance().removeNamespace(namespaceInfo.namespaceId);
    Tables.clearCache(master.getInstance());
    Utils.unreserveNamespace(namespaceInfo.namespaceId, tid, true);
  }

}

class SetupNamespacePermissions extends MasterRepo {

  private static final long serialVersionUID = 1L;

  private NamespaceInfo namespaceInfo;

  public SetupNamespacePermissions(NamespaceInfo ti) {
    this.namespaceInfo = ti;
  }

  @Override
  public Repo<Master> call(long tid, Master env) throws Exception {
    // give all namespace permissions to the creator
    SecurityOperation security = AuditedSecurityOperation.getInstance();
    for (NamespacePermission permission : NamespacePermission.values()) {
      try {
        security.grantNamespacePermission(SystemCredentials.get().toThrift(env.getInstance()), namespaceInfo.user, namespaceInfo.namespaceId, permission);
      } catch (ThriftSecurityException e) {
        Logger.getLogger(FinishCreateNamespace.class).error(e.getMessage(), e);
        throw e;
      }
    }

    // setup permissions in zookeeper before table info in zookeeper
    // this way concurrent users will not get a spurious permission denied
    // error
    return new PopulateZookeeperWithNamespace(namespaceInfo);
  }
}

public class CreateNamespace extends MasterRepo {
  private static final long serialVersionUID = 1L;

  private NamespaceInfo namespaceInfo;

  public CreateNamespace(String user, String namespaceName, Map<String,String> props) {
    namespaceInfo = new NamespaceInfo();
    namespaceInfo.namespaceName = namespaceName;
    namespaceInfo.user = user;
    namespaceInfo.props = props;
  }

  @Override
  public long isReady(long tid, Master environment) throws Exception {
    return 0;
  }

  @Override
  public Repo<Master> call(long tid, Master master) throws Exception {
    Utils.idLock.lock();
    try {
      namespaceInfo.namespaceId = Utils.getNextTableId(namespaceInfo.namespaceName, master.getInstance());
      return new SetupNamespacePermissions(namespaceInfo);
    } finally {
      Utils.idLock.unlock();
    }

  }

  @Override
  public void undo(long tid, Master env) throws Exception {
    // nothing to do, the namespace id was allocated!
  }

}
