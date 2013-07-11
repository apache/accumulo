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
import java.util.Set;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.thrift.TableOperation;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.tables.TableManager;
import org.apache.log4j.Logger;

class CloneNamespaceInfo implements Serializable {

  private static final long serialVersionUID = 1L;

  String srcId;
  String namespace;
  String newId;
  Map<String,String> propertiesToSet;
  Set<String> propertiesToExclude;

  public String user;
}

class FinishCloneTableNamespace extends MasterRepo {

  private static final long serialVersionUID = 1L;
  private CloneNamespaceInfo cloneInfo;

  public FinishCloneTableNamespace(CloneNamespaceInfo cloneInfo) {
    this.cloneInfo = cloneInfo;
  }

  @Override
  public long isReady(long tid, Master environment) throws Exception {
    return 0;
  }

  @Override
  public Repo<Master> call(long tid, Master environment) throws Exception {
    Utils.unreserveTableNamespace(cloneInfo.srcId, tid, false);
    Utils.unreserveTableNamespace(cloneInfo.newId, tid, true);

    environment.getEventCoordinator().event("Cloned table namespace %s from %s", cloneInfo.namespace, cloneInfo.srcId);

    Logger.getLogger(FinishCloneTableNamespace.class).debug("Cloned table namespace " + cloneInfo.srcId + " " + cloneInfo.newId + " " + cloneInfo.namespace);

    return null;
  }

  @Override
  public void undo(long tid, Master environment) throws Exception {}
}

class CloneNamespaceZookeeper extends MasterRepo {

  private static final long serialVersionUID = 1L;

  private CloneNamespaceInfo cloneInfo;

  public CloneNamespaceZookeeper(CloneNamespaceInfo cloneInfo) {
    this.cloneInfo = cloneInfo;
  }

  @Override
  public long isReady(long tid, Master environment) throws Exception {
    return Utils.reserveTableNamespace(cloneInfo.newId, tid, true, false, TableOperation.CLONE);
  }

  @Override
  public Repo<Master> call(long tid, Master environment) throws Exception {
    Utils.tableNameLock.lock();
    try {
      // write namespace to zookeeper
      Instance instance = HdfsZooInstance.getInstance();

      Utils.checkTableNamespaceDoesNotExist(instance, cloneInfo.namespace, cloneInfo.newId, TableOperation.CLONE);

      TableManager.getInstance().addNamespace(cloneInfo.newId, cloneInfo.namespace, NodeExistsPolicy.FAIL);
      TableManager.getInstance().cloneNamespace(cloneInfo.srcId, cloneInfo.newId, cloneInfo.namespace, cloneInfo.propertiesToSet,
          cloneInfo.propertiesToExclude, NodeExistsPolicy.OVERWRITE);
      Tables.clearCache(instance);

      return new FinishCloneTableNamespace(cloneInfo);
    } finally {
      Utils.tableNameLock.unlock();
    }
  }

  @Override
  public void undo(long tid, Master environment) throws Exception {
    Instance instance = HdfsZooInstance.getInstance();
    TableManager.getInstance().removeNamespace(cloneInfo.newId);
    Utils.unreserveTableNamespace(cloneInfo.newId, tid, true);
    Tables.clearCache(instance);
  }
}

class CloneNamespacePermissions extends MasterRepo {

  private static final long serialVersionUID = 1L;

  private CloneNamespaceInfo cloneInfo;

  public CloneNamespacePermissions(CloneNamespaceInfo cloneInfo) {
    this.cloneInfo = cloneInfo;
  }

  @Override
  public long isReady(long tid, Master environment) throws Exception {
    return 0;
  }

  @Override
  public Repo<Master> call(long tid, Master environment) throws Exception {
    // TODO add table namespace permissions (ACCUMULO-1479)
    // give all table permissions to the creator
    /*
     * for (TablePermission permission : TablePermission.values()) { try {
     * AuditedSecurityOperation.getInstance().grantTablePermission(SecurityConstants.getSystemCredentials(), cloneInfo.user, cloneInfo.newId, permission); }
     * catch (ThriftSecurityException e) { Logger.getLogger(FinishCloneTableNamespace.class).error(e.getMessage(), e); throw e; } }
     */

    // setup permissions in zookeeper before table info in zookeeper
    // this way concurrent users will not get a spurious pemission denied
    // error
    return new CloneNamespaceZookeeper(cloneInfo);
  }

  @Override
  public void undo(long tid, Master environment) throws Exception {

  }
}

public class CloneTableNamespace extends MasterRepo {

  private static final long serialVersionUID = 1L;
  private CloneNamespaceInfo cloneInfo;

  public CloneTableNamespace(String user, String srcId, String namespace, Map<String,String> propertiesToSet, Set<String> propertiesToExclude) {
    cloneInfo = new CloneNamespaceInfo();
    cloneInfo.user = user;
    cloneInfo.srcId = srcId;
    cloneInfo.namespace = namespace;
    cloneInfo.propertiesToExclude = propertiesToExclude;
    cloneInfo.propertiesToSet = propertiesToSet;
  }

  @Override
  public long isReady(long tid, Master environment) throws Exception {
    return Utils.reserveTableNamespace(cloneInfo.srcId, tid, false, true, TableOperation.CLONE);
  }

  @Override
  public Repo<Master> call(long tid, Master environment) throws Exception {

    Utils.idLock.lock();
    try {
      Instance instance = HdfsZooInstance.getInstance();
      cloneInfo.newId = Utils.getNextTableId(cloneInfo.namespace, instance);
      return new CloneNamespacePermissions(cloneInfo);
    } finally {
      Utils.idLock.unlock();
    }
  }

  @Override
  public void undo(long tid, Master environment) throws Exception {
    Utils.unreserveTableNamespace(cloneInfo.srcId, tid, false);
  }

}
