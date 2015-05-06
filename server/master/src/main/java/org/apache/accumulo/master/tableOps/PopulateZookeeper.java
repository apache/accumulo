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

import java.util.Map.Entry;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.thrift.TableOperation;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.tables.TableManager;
import org.apache.accumulo.server.util.TablePropUtil;

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
