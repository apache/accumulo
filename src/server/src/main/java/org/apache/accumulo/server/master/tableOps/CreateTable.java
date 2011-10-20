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
package org.apache.accumulo.server.master.tableOps;

import java.io.Serializable;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.thrift.TableOperation;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.file.FileUtil;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.server.fate.Repo;
import org.apache.accumulo.server.master.Master;
import org.apache.accumulo.server.master.state.tables.TableManager;
import org.apache.accumulo.server.security.Authenticator;
import org.apache.accumulo.server.security.SecurityConstants;
import org.apache.accumulo.server.security.ZKAuthenticator;
import org.apache.accumulo.server.tabletserver.TabletTime;
import org.apache.accumulo.server.trace.TraceFileSystem;
import org.apache.accumulo.server.util.MetadataTable;
import org.apache.accumulo.server.util.TablePropUtil;
import org.apache.accumulo.server.util.TabletOperations;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

class TableInfo implements Serializable {
  
  private static final long serialVersionUID = 1L;
  
  String tableName;
  String tableId;
  char timeType;
  String user;
  
  public Map<String,String> props;
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
    MetadataTable.addTablet(extent, Constants.DEFAULT_TABLET_LOCATION, SecurityConstants.getSystemCredentials(), tableInfo.timeType,
        environment.getMasterLock());
    
    return new FinishCreateTable(tableInfo);
    
  }
  
  @Override
  public void undo(long tid, Master environment) throws Exception {
    MetadataTable.deleteTable(tableInfo.tableId, false, SecurityConstants.getSystemCredentials(), environment.getMasterLock());
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
  public Repo<Master> call(long tid, Master environment) throws Exception {
    FileSystem fs = TraceFileSystem.wrap(FileUtil.getFileSystem(CachedConfiguration.getInstance(), ServerConfiguration.getSiteConfiguration()));
    String dir = ServerConstants.getTablesDir() + "/" + tableInfo.tableId;
    TabletOperations.createTabletDirectory(fs, dir, null);
    return new PopulateMetadata(tableInfo);
  }
  
  @Override
  public void undo(long tid, Master environment) throws Exception {
    FileSystem fs = TraceFileSystem.wrap(FileUtil.getFileSystem(CachedConfiguration.getInstance(), ServerConfiguration.getSiteConfiguration()));
    String dir = ServerConstants.getTablesDir() + "/" + tableInfo.tableId;
    fs.delete(new Path(dir), true);
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
  public Repo<Master> call(long tid, Master env) throws Exception {
    // reserve the table name in zookeeper or fail
    
    Utils.tableNameLock.lock();
    try {
      // write tableName & tableId to zookeeper
      Instance instance = HdfsZooInstance.getInstance();
      
      Utils.checkTableDoesNotExist(instance, tableInfo.tableName, tableInfo.tableId, TableOperation.CREATE);
      
      TableManager.getInstance().addTable(tableInfo.tableId, tableInfo.tableName, NodeExistsPolicy.OVERWRITE);
      
      for (Entry<String,String> entry : tableInfo.props.entrySet())
        TablePropUtil.setTableProperty(tableInfo.tableId, entry.getKey(), entry.getValue());
      
      Tables.clearCache(instance);
      return new CreateDir(tableInfo);
    } finally {
      Utils.tableNameLock.unlock();
    }
    
  }
  
  @Override
  public void undo(long tid, Master env) throws Exception {
    Instance instance = HdfsZooInstance.getInstance();
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
  public long isReady(long tid, Master environment) throws Exception {
    return 0;
  }
  
  @Override
  public Repo<Master> call(long tid, Master env) throws Exception {
    // give all table permissions to the creator
    Authenticator authenticator = ZKAuthenticator.getInstance();
    for (TablePermission permission : TablePermission.values()) {
      try {
        authenticator.grantTablePermission(SecurityConstants.getSystemCredentials(), tableInfo.user, tableInfo.tableId, permission);
      } catch (AccumuloSecurityException e) {
        Logger.getLogger(FinishCreateTable.class).error(e.getMessage(), e);
        throw e.asThriftException();
      }
    }
    
    // setup permissions in zookeeper before table info in zookeeper
    // this way concurrent users will not get a spurious permission denied
    // error
    return new PopulateZookeeper(tableInfo);
  }
  
  @Override
  public void undo(long tid, Master env) throws Exception {
    ZKAuthenticator.getInstance().deleteTable(SecurityConstants.getSystemCredentials(), tableInfo.tableId);
  }
  
}

public class CreateTable extends MasterRepo {
  private static final long serialVersionUID = 1L;
  
  private TableInfo tableInfo;
  
  public CreateTable(String user, String tableName, TimeType timeType, Map<String,String> props) {
    tableInfo = new TableInfo();
    tableInfo.tableName = tableName;
    tableInfo.timeType = TabletTime.getTimeID(timeType);
    tableInfo.user = user;
    tableInfo.props = props;
  }
  
  @Override
  public long isReady(long tid, Master environment) throws Exception {
    return 0;
  }
  
  @Override
  public Repo<Master> call(long tid, Master env) throws Exception {
    // first step is to reserve a table id.. if the machine fails during this step
    // it is ok to retry... the only side effect is that a table id may not be used
    // or skipped
    
    // assuming only the master process is creating tables
    
    Utils.idLock.lock();
    try {
      Instance instance = HdfsZooInstance.getInstance();
      tableInfo.tableId = Utils.getNextTableId(tableInfo.tableName, instance);
      return new SetupPermissions(tableInfo);
    } finally {
      Utils.idLock.unlock();
    }
    
  }
  
  @Override
  public void undo(long tid, Master env) throws Exception {
    // nothing to do, the table id was allocated!
  }
  
}
