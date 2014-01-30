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
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.impl.Namespaces;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.thrift.TableOperation;
import org.apache.accumulo.core.client.impl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.client.impl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.security.AuditedSecurityOperation;
import org.apache.accumulo.server.security.SystemCredentials;
import org.apache.accumulo.server.tables.TableManager;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.log4j.Logger;

class CloneInfo implements Serializable {

  private static final long serialVersionUID = 1L;

  String srcTableId;
  String tableName;
  String tableId;
  String namespaceId;
  String srcNamespaceId;
  Map<String,String> propertiesToSet;
  Set<String> propertiesToExclude;

  public String user;
}

class FinishCloneTable extends MasterRepo {

  private static final long serialVersionUID = 1L;
  private CloneInfo cloneInfo;

  public FinishCloneTable(CloneInfo cloneInfo) {
    this.cloneInfo = cloneInfo;
  }

  @Override
  public long isReady(long tid, Master environment) throws Exception {
    return 0;
  }

  @Override
  public Repo<Master> call(long tid, Master environment) throws Exception {
    // directories are intentionally not created.... this is done because directories should be unique
    // because they occupy a different namespace than normal tablet directories... also some clones
    // may never create files.. therefore there is no need to consume namenode space w/ directories
    // that are not used... tablet will create directories as needed

    TableManager.getInstance().transitionTableState(cloneInfo.tableId, TableState.ONLINE);

    Utils.unreserveNamespace(cloneInfo.srcNamespaceId, tid, false);
    if (!cloneInfo.srcNamespaceId.equals(cloneInfo.namespaceId))
      Utils.unreserveNamespace(cloneInfo.namespaceId, tid, false);
    Utils.unreserveTable(cloneInfo.srcTableId, tid, false);
    Utils.unreserveTable(cloneInfo.tableId, tid, true);

    environment.getEventCoordinator().event("Cloned table %s from %s", cloneInfo.tableName, cloneInfo.srcTableId);

    Logger.getLogger(FinishCloneTable.class).debug("Cloned table " + cloneInfo.srcTableId + " " + cloneInfo.tableId + " " + cloneInfo.tableName);

    return null;
  }

  @Override
  public void undo(long tid, Master environment) throws Exception {}

}

class CloneMetadata extends MasterRepo {

  private static final long serialVersionUID = 1L;
  private CloneInfo cloneInfo;

  public CloneMetadata(CloneInfo cloneInfo) {
    this.cloneInfo = cloneInfo;
  }

  @Override
  public long isReady(long tid, Master environment) throws Exception {
    return 0;
  }

  @Override
  public Repo<Master> call(long tid, Master environment) throws Exception {
    Logger.getLogger(CloneMetadata.class).info(
        String.format("Cloning %s with tableId %s from srcTableId %s", cloneInfo.tableName, cloneInfo.tableId, cloneInfo.srcTableId));
    Instance instance = HdfsZooInstance.getInstance();
    // need to clear out any metadata entries for tableId just in case this
    // died before and is executing again
    MetadataTableUtil.deleteTable(cloneInfo.tableId, false, SystemCredentials.get(), environment.getMasterLock());
    MetadataTableUtil.cloneTable(instance, cloneInfo.srcTableId, cloneInfo.tableId, environment.getFileSystem());
    return new FinishCloneTable(cloneInfo);
  }

  @Override
  public void undo(long tid, Master environment) throws Exception {
    MetadataTableUtil.deleteTable(cloneInfo.tableId, false, SystemCredentials.get(), environment.getMasterLock());
  }

}

class CloneZookeeper extends MasterRepo {

  private static final long serialVersionUID = 1L;

  private CloneInfo cloneInfo;

  public CloneZookeeper(CloneInfo cloneInfo) throws NamespaceNotFoundException {
    this.cloneInfo = cloneInfo;
    Instance inst = HdfsZooInstance.getInstance();
    this.cloneInfo.namespaceId = Namespaces.getNamespaceId(inst, Tables.qualify(this.cloneInfo.tableName).getFirst());
  }

  @Override
  public long isReady(long tid, Master environment) throws Exception {
    long val = 0;
    if (!cloneInfo.srcNamespaceId.equals(cloneInfo.namespaceId))
      val += Utils.reserveNamespace(cloneInfo.namespaceId, tid, false, true, TableOperation.CLONE);
    val += Utils.reserveTable(cloneInfo.tableId, tid, true, false, TableOperation.CLONE);
    return val;
  }

  @Override
  public Repo<Master> call(long tid, Master environment) throws Exception {
    Utils.tableNameLock.lock();
    try {
      // write tableName & tableId to zookeeper
      Instance instance = HdfsZooInstance.getInstance();

      Utils.checkTableDoesNotExist(instance, cloneInfo.tableName, cloneInfo.tableId, TableOperation.CLONE);

      TableManager.getInstance().cloneTable(cloneInfo.srcTableId, cloneInfo.tableId, cloneInfo.tableName, cloneInfo.namespaceId, cloneInfo.propertiesToSet,
          cloneInfo.propertiesToExclude, NodeExistsPolicy.OVERWRITE);
      Tables.clearCache(instance);

      return new CloneMetadata(cloneInfo);
    } finally {
      Utils.tableNameLock.unlock();
    }
  }

  @Override
  public void undo(long tid, Master environment) throws Exception {
    Instance instance = HdfsZooInstance.getInstance();
    TableManager.getInstance().removeTable(cloneInfo.tableId);
    if (!cloneInfo.srcNamespaceId.equals(cloneInfo.namespaceId))
      Utils.unreserveNamespace(cloneInfo.namespaceId, tid, false);
    Utils.unreserveTable(cloneInfo.tableId, tid, true);
    Tables.clearCache(instance);
  }

}

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
        AuditedSecurityOperation.getInstance().grantTablePermission(SystemCredentials.get().toThrift(environment.getInstance()), cloneInfo.user,
            cloneInfo.tableId, permission, cloneInfo.namespaceId);
      } catch (ThriftSecurityException e) {
        Logger.getLogger(FinishCloneTable.class).error(e.getMessage(), e);
        throw e;
      }
    }

    // setup permissions in zookeeper before table info in zookeeper
    // this way concurrent users will not get a spurious pemission denied
    // error
    try {
      return new CloneZookeeper(cloneInfo);
    } catch (NamespaceNotFoundException e) {
      throw new ThriftTableOperationException(null, cloneInfo.tableName, TableOperation.CLONE, TableOperationExceptionType.NAMESPACE_NOTFOUND,
          "Namespace for target table not found");
    }
  }

  @Override
  public void undo(long tid, Master environment) throws Exception {
    AuditedSecurityOperation.getInstance().deleteTable(SystemCredentials.get().toThrift(environment.getInstance()), cloneInfo.tableId, cloneInfo.namespaceId);
  }
}

public class CloneTable extends MasterRepo {

  private static final long serialVersionUID = 1L;
  private CloneInfo cloneInfo;

  public CloneTable(String user, String srcTableId, String tableName, Map<String,String> propertiesToSet, Set<String> propertiesToExclude) {
    cloneInfo = new CloneInfo();
    cloneInfo.user = user;
    cloneInfo.srcTableId = srcTableId;
    cloneInfo.tableName = tableName;
    cloneInfo.propertiesToExclude = propertiesToExclude;
    cloneInfo.propertiesToSet = propertiesToSet;
    Instance inst = HdfsZooInstance.getInstance();
    cloneInfo.srcNamespaceId = Tables.getNamespaceId(inst, cloneInfo.srcTableId);
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
      Instance instance = HdfsZooInstance.getInstance();
      cloneInfo.tableId = Utils.getNextTableId(cloneInfo.tableName, instance);
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
