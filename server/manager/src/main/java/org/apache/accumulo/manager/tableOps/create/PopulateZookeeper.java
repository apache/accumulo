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
package org.apache.accumulo.manager.tableOps.create;

import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.clientImpl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.clientImpl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.manager.tableOps.TableInfo;
import org.apache.accumulo.manager.tableOps.Utils;
import org.apache.accumulo.server.conf.store.TablePropKey;
import org.apache.accumulo.server.util.PropUtil;

class PopulateZookeeper extends ManagerRepo {

  private static final long serialVersionUID = 1L;

  private final TableInfo tableInfo;

  PopulateZookeeper(TableInfo ti) {
    this.tableInfo = ti;
  }

  @Override
  public long isReady(long tid, Manager environment) throws Exception {
    return Utils.reserveTable(environment, tableInfo.getTableId(), tid, true, false,
        TableOperation.CREATE);
  }

  @Override
  public Repo<Manager> call(long tid, Manager manager) throws Exception {
    // reserve the table name in zookeeper or fail

    Utils.getTableNameLock().lock();
    try {
      // write tableName & tableId to zookeeper
      Utils.checkTableDoesNotExist(manager.getContext(), tableInfo.getTableName(),
          tableInfo.getTableId(), TableOperation.CREATE);

      manager.getTableManager().addTable(tableInfo.getTableId(), tableInfo.getNamespaceId(),
          tableInfo.getTableName());

      try {
        PropUtil.setProperties(manager.getContext(),
            TablePropKey.of(manager.getContext(), tableInfo.getTableId()), tableInfo.props);
      } catch (IllegalStateException ex) {
        throw new ThriftTableOperationException(null, tableInfo.getTableName(),
            TableOperation.CREATE, TableOperationExceptionType.OTHER,
            "Property or value not valid for create " + tableInfo.getTableName() + " in "
                + tableInfo.props);
      }

      manager.getContext().clearTableListCache();
      return new ChooseDir(tableInfo);
    } finally {
      Utils.getTableNameLock().unlock();
    }

  }

  @Override
  public void undo(long tid, Manager manager) throws Exception {
    manager.getTableManager().removeTable(tableInfo.getTableId());
    Utils.unreserveTable(manager, tableInfo.getTableId(), tid, true);
    manager.getContext().clearTableListCache();
  }

}
