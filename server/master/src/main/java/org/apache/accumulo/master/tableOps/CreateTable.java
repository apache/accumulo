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

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.thrift.TableOperation;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.security.AuditedSecurityOperation;
import org.apache.accumulo.server.security.SecurityOperation;
import org.apache.accumulo.server.security.SystemCredentials;
import org.apache.accumulo.server.tables.TableManager;
import org.apache.accumulo.server.tablets.TabletTime;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.accumulo.server.util.TablePropUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

class TableInfo implements Serializable {

  private static final long serialVersionUID = 1L;

  String tableName;
  String tableId;
  String namespaceId;
  char timeType;
  String user;

  public Map<String,String> props;

  public String dir = null;
}

class FinishCreateTable extends MasterRepo {

  private static final long serialVersionUID = 1L;

  private TableInfo tableInfo;

  public FinishCreateTable(TableInfo ti) {
    this.tableInfo = ti;
  }

  @Override
  public long isReady(long tid, Master environment) throws Exception {
    return 0;
  }

  @Override
  public Repo<Master> call(long tid, Master env) throws Exception {
    TableManager.getInstance().transitionTableState(tableInfo.tableId, TableState.ONLINE);

    Utils.unreserveNamespace(tableInfo.namespaceId, tid, false);
    Utils.unreserveTable(tableInfo.tableId, tid, true);

    env.getEventCoordinator().event("Created table %s ", tableInfo.tableName);

    Logger.getLogger(FinishCreateTable.class).debug("Created table " + tableInfo.tableId + " " + tableInfo.tableName);

    return null;
  }

  @Override
  public String getReturn() {
    return tableInfo.tableId;
  }

  @Override
  public void undo(long tid, Master env) throws Exception {}

}

class PopulateMetadata extends MasterRepo {

  private static final long serialVersionUID = 1L;

  private TableInfo tableInfo;

  PopulateMetadata(TableInfo ti) {
    this.tableInfo = ti;
  }

  @Override
  public long isReady(long tid, Master environment) throws Exception {
    return 0;
  }

  @Override
  public Repo<Master> call(long tid, Master environment) throws Exception {
    KeyExtent extent = new KeyExtent(new Text(tableInfo.tableId), null, null);
    MetadataTableUtil.addTablet(extent, tableInfo.dir, SystemCredentials.get(), tableInfo.timeType, environment.getMasterLock());

    return new FinishCreateTable(tableInfo);

  }

  @Override
  public void undo(long tid, Master environment) throws Exception {
    MetadataTableUtil.deleteTable(tableInfo.tableId, false, SystemCredentials.get(), environment.getMasterLock());
  }

}

class CreateDir extends MasterRepo {
  private static final long serialVersionUID = 1L;

  private TableInfo tableInfo;

  CreateDir(TableInfo ti) {
    this.tableInfo = ti;
  }

  @Override
  public long isReady(long tid, Master environment) throws Exception {
    return 0;
  }

  @Override
  public Repo<Master> call(long tid, Master master) throws Exception {
    VolumeManager fs = master.getFileSystem();
    fs.mkdirs(new Path(tableInfo.dir));
    return new PopulateMetadata(tableInfo);
  }

  @Override
  public void undo(long tid, Master master) throws Exception {
    VolumeManager fs = master.getFileSystem();
    fs.deleteRecursively(new Path(tableInfo.dir));

  }
}

class ChooseDir extends MasterRepo {
  private static final long serialVersionUID = 1L;

  private TableInfo tableInfo;

  ChooseDir(TableInfo ti) {
    this.tableInfo = ti;
  }

  @Override
  public long isReady(long tid, Master environment) throws Exception {
    return 0;
  }

  @Override
  public Repo<Master> call(long tid, Master master) throws Exception {
    // Constants.DEFAULT_TABLET_LOCATION has a leading slash prepended to it so we don't need to add one here
    tableInfo.dir = master.getFileSystem().choose(ServerConstants.getBaseUris()) + Constants.HDFS_TABLES_DIR + Path.SEPARATOR + tableInfo.tableId
        + Constants.DEFAULT_TABLET_LOCATION;
    return new CreateDir(tableInfo);
  }

  @Override
  public void undo(long tid, Master master) throws Exception {

  }
}

class PopulateZookeeper extends MasterRepo {

  private static final long serialVersionUID = 1L;

  private TableInfo tableInfo;

  PopulateZookeeper(TableInfo ti) {
    this.tableInfo = ti;
  }

  @Override
  public long isReady(long tid, Master environment) throws Exception {
    return Utils.reserveTable(tableInfo.tableId, tid, true, false, TableOperation.CREATE);
  }

  @Override
  public Repo<Master> call(long tid, Master master) throws Exception {
    // reserve the table name in zookeeper or fail

    Utils.tableNameLock.lock();
    try {
      // write tableName & tableId to zookeeper
      Instance instance = master.getInstance();

      Utils.checkTableDoesNotExist(instance, tableInfo.tableName, tableInfo.tableId, TableOperation.CREATE);

      TableManager.getInstance().addTable(tableInfo.tableId, tableInfo.namespaceId, tableInfo.tableName, NodeExistsPolicy.OVERWRITE);

      for (Entry<String,String> entry : tableInfo.props.entrySet())
        TablePropUtil.setTableProperty(tableInfo.tableId, entry.getKey(), entry.getValue());

      Tables.clearCache(instance);
      return new ChooseDir(tableInfo);
    } finally {
      Utils.tableNameLock.unlock();
    }

  }

  @Override
  public void undo(long tid, Master master) throws Exception {
    Instance instance = master.getInstance();
    TableManager.getInstance().removeTable(tableInfo.tableId);
    Utils.unreserveTable(tableInfo.tableId, tid, true);
    Tables.clearCache(instance);
  }

}

class SetupPermissions extends MasterRepo {

  private static final long serialVersionUID = 1L;

  private TableInfo tableInfo;

  public SetupPermissions(TableInfo ti) {
    this.tableInfo = ti;
  }

  @Override
  public Repo<Master> call(long tid, Master env) throws Exception {
    // give all table permissions to the creator
    SecurityOperation security = AuditedSecurityOperation.getInstance();
    for (TablePermission permission : TablePermission.values()) {
      try {
        security
            .grantTablePermission(SystemCredentials.get().toThrift(env.getInstance()), tableInfo.user, tableInfo.tableId, permission, tableInfo.namespaceId);
      } catch (ThriftSecurityException e) {
        Logger.getLogger(FinishCreateTable.class).error(e.getMessage(), e);
        throw e;
      }
    }

    // setup permissions in zookeeper before table info in zookeeper
    // this way concurrent users will not get a spurious permission denied
    // error
    return new PopulateZookeeper(tableInfo);
  }

  @Override
  public void undo(long tid, Master env) throws Exception {
    AuditedSecurityOperation.getInstance().deleteTable(SystemCredentials.get().toThrift(env.getInstance()), tableInfo.tableId, tableInfo.namespaceId);
  }

}

public class CreateTable extends MasterRepo {
  private static final long serialVersionUID = 1L;

  private TableInfo tableInfo;

  public CreateTable(String user, String tableName, TimeType timeType, Map<String,String> props, String namespaceId) {
    tableInfo = new TableInfo();
    tableInfo.tableName = tableName;
    tableInfo.timeType = TabletTime.getTimeID(timeType);
    tableInfo.user = user;
    tableInfo.props = props;
    tableInfo.namespaceId = namespaceId;
  }

  @Override
  public long isReady(long tid, Master environment) throws Exception {
    // reserve the table's namespace to make sure it doesn't change while the table is created
    return Utils.reserveNamespace(tableInfo.namespaceId, tid, false, true, TableOperation.CREATE);
  }

  @Override
  public Repo<Master> call(long tid, Master master) throws Exception {
    // first step is to reserve a table id.. if the machine fails during this step
    // it is ok to retry... the only side effect is that a table id may not be used
    // or skipped

    // assuming only the master process is creating tables

    Utils.idLock.lock();
    try {
      tableInfo.tableId = Utils.getNextTableId(tableInfo.tableName, master.getInstance());
      return new SetupPermissions(tableInfo);
    } finally {
      Utils.idLock.unlock();
    }

  }

  @Override
  public void undo(long tid, Master env) throws Exception {
    Utils.unreserveNamespace(tableInfo.namespaceId, tid, false);
  }

}
