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
package org.apache.accumulo.master.tableOps.create;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import org.apache.accumulo.core.client.admin.InitialTableState;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.fate.FateTxId;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.master.tableOps.MasterRepo;
import org.apache.accumulo.master.tableOps.TableInfo;
import org.apache.accumulo.master.tableOps.Utils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateTable extends MasterRepo {
  private static final long serialVersionUID = 1L;
  private static final Logger log = LoggerFactory.getLogger(CreateTable.class);

  private TableInfo tableInfo;

  public CreateTable(String user, String tableName, TimeType timeType, Map<String,String> props,
      Path splitPath, int splitCount, Path splitDirsPath, InitialTableState initialTableState,
      NamespaceId namespaceId) {
    tableInfo = new TableInfo();
    tableInfo.setTableName(tableName);
    tableInfo.setTimeType(timeType);
    tableInfo.setUser(user);
    tableInfo.props = props;
    tableInfo.setNamespaceId(namespaceId);
    tableInfo.setSplitPath(splitPath);
    tableInfo.setInitialSplitSize(splitCount);
    tableInfo.setInitialTableState(initialTableState);
    tableInfo.setSplitDirsPath(splitDirsPath);
  }

  @Override
  public long isReady(long tid, Master environment) throws Exception {
    // reserve the table's namespace to make sure it doesn't change while the table is created
    return Utils.reserveNamespace(environment, tableInfo.getNamespaceId(), tid, false, true,
        TableOperation.CREATE);
  }

  @Override
  public Repo<Master> call(long tid, Master master) throws Exception {
    // first step is to reserve a table id.. if the machine fails during this step
    // it is ok to retry... the only side effect is that a table id may not be used
    // or skipped

    // assuming only the master process is creating tables

    Utils.getIdLock().lock();
    try {
      String tName = tableInfo.getTableName();
      tableInfo.setTableId(Utils.getNextId(tName, master.getContext(), TableId::of));
      return new SetupPermissions(tableInfo);
    } finally {
      Utils.getIdLock().unlock();
    }
  }

  @Override
  public void undo(long tid, Master env) throws IOException {
    // Clean up split files if create table operation fails
    try {
      if (tableInfo.getInitialSplitSize() > 0) {
        Path p = tableInfo.getSplitPath().getParent();
        FileSystem fs = p.getFileSystem(env.getContext().getHadoopConf());
        fs.delete(p, true);
      }
    } catch (NullPointerException | IOException e) {
      var spdir = Optional.ofNullable(tableInfo).map(TableInfo::getSplitDirsPath).orElse(null);
      log.error("{} Failed to undo CreateTable operation, split dir {} ", FateTxId.formatTid(tid),
          spdir, e);
    } finally {
      Utils.unreserveNamespace(env, tableInfo.getNamespaceId(), tid, false);
    }
  }

}
